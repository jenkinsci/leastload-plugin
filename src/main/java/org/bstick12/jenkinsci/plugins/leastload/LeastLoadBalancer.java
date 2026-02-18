/*
 * The MIT License
 *
 * Copyright (c) 2013, Brendan Nolan
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.bstick12.jenkinsci.plugins.leastload;

import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.init.Initializer;
import hudson.model.Computer;
import hudson.model.Executor;
import hudson.model.Job;
import hudson.model.Label;
import hudson.model.LoadBalancer;
import hudson.model.Node;
import hudson.model.Queue.Task;
import hudson.model.queue.MappingWorksheet;
import hudson.model.queue.MappingWorksheet.ExecutorChunk;
import hudson.model.queue.MappingWorksheet.Mapping;
import hudson.model.queue.MappingWorksheet.WorkChunk;
import hudson.model.queue.LoadPredictor;
import hudson.model.queue.SubTask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import jenkins.model.Jenkins;

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINER;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

/**
 * A {@link LoadBalancer} implementation that the leastload plugin uses to replace the default
 * Jenkins {@link LoadBalancer}
 * <p>The {@link LeastLoadBalancer} chooses {@link Executor}s that have the least load. An {@link Executor} is defined
 * as having the least load if it is idle or has the most available {@link Executor}s
 * <p>Only executor chunks that are clearly available (node non-null, not offline) are considered.
 * The balancer tracks nodes that have not yet been assigned work this "round"; it assigns to a known-empty node
 * uniquely. When all tracked nodes have been used, it re-checks Jenkins for currently available nodes (online,
 * accepting tasks, with idle executors) and starts a new round. This spreads load and avoids multiple jobs
 * piling on the same agent before queue maintenance runs again.
 * <p>When least-load cannot produce a mapping, it returns null so the task remains in the queue for the next cycle.
 * The fallback load balancer is used only when least-load is disabled for the job via {@link LeastLoadDisabledProperty}.
 * <p>When work requests a label and the plugin has a matching free node, it assigns that node immediately
 * (even if it was already used this round) to avoid long delays when only one node has the label.
 *
 * @author brendan.nolan@gmail.com
 */
public class LeastLoadBalancer extends LoadBalancer {

    private static final Logger LOGGER = Logger.getLogger(LeastLoadBalancer.class.getName());

    private static final Comparator<ExecutorChunk> EXECUTOR_CHUNK_COMPARATOR = Collections.reverseOrder(new ExecutorChunkComparator());

    private static String tracePrefix(String traceId) {
        return "[trace-" + traceId + "] ";
    }

    private final LoadBalancer fallback;

    /**
     * Nodes considered available for assignment this round (name → node). When empty, we refresh from Jenkins
     * (nodes that are online, accepting tasks, with idle executors). Tracks Node references so we can match
     * work labels to nodes. Access only under Queue lock (map() is serialized by the caller).
     */
    @SuppressFBWarnings(value = "MS_MUTABLE_COLLECTION", justification = "Access only under Queue lock (map() is serialized by the caller); not exposed")
    private final Map<String, Node> availableNodesThisRound = new HashMap<>();

    /**
     * Per-label sets of node names available this round (label display name → node names). Populated on refresh
     * so that high-demand labels (e.g. x86_64_medium) have their own capacity and are not starved by a single
     * global round shared with other labels. When empty, refreshed together with {@link #availableNodesThisRound}.
     */
    @SuppressFBWarnings(value = "MS_MUTABLE_COLLECTION", justification = "Access only under Queue lock (map() is serialized by the caller); not exposed")
    private final Map<String, Set<String>> availableNodesThisRoundByLabel = new HashMap<>();

    /**
     * Create the {@link LeastLoadBalancer} with a fallback that will be
     * used in case of any failures.
     *
     * @param fallback The {@link LoadBalancer} fallback to use in case of failure
     */
    public LeastLoadBalancer(LoadBalancer fallback) {
        Preconditions.checkNotNull(fallback, "You must provide a fallback implementation of the LoadBalancer");
        this.fallback = fallback;
    }

    @Initializer
    public static void register() {
        var queue = Jenkins.get().getQueue();
        queue.setLoadBalancer(new LeastLoadBalancer(queue.getLoadBalancer()));
    }

    /**
     * When running with a Jenkins core that uses {@link LoadBalancer#getLoadPredictors()}, return no predictors
     * so that MappingWorksheet skips load prediction. If the core does not define this method, this is just an
     * additional method on the balancer and is never called.
     */
    public Collection<LoadPredictor> getLoadPredictors() {
        return Collections.emptyList();
    }

    @Override
    @CheckForNull
    public Mapping map(@NonNull Task task, MappingWorksheet ws) {
        long startNanos = System.nanoTime();
        String trace_id = String.format("%08x", (int) (System.nanoTime() & 0xFFFFFFFF));

        try {

            if (!isDisabled(task)) {

                if (availableNodesThisRound.isEmpty()) {
                    refreshAvailableNodes(trace_id);
                } else if (isLabelSetEmptyForTask(ws)) {
                    refreshAvailableNodes(trace_id);
                }

                List<ExecutorChunk> useableChunks = getApplicableSortedByLoad(ws);
                Set<String> nodeNamesForFilter = getNodeNamesForFilter(ws);
                List<ExecutorChunk> chunksForThisRound = filterToAvailableNodesThisRound(useableChunks, nodeNamesForFilter);

                if (chunksForThisRound.isEmpty()) {
                    refreshAvailableNodes(trace_id);
                    nodeNamesForFilter = getNodeNamesForFilter(ws);
                    chunksForThisRound = filterToAvailableNodesThisRound(useableChunks, nodeNamesForFilter);
                }
                if (chunksForThisRound.isEmpty() && !useableChunks.isEmpty() && isLabelRestrictedWork(ws)) {
                    // Only immediate-assign if at least one chunk is on a node we haven't assigned this round.
                    // Otherwise we'd double-assign the same executor (e.g. same maintain() or executor not started yet).
                    Set<String> availableNames = availableNodesThisRound.keySet();
                    boolean anyStillAvailable = false;
                    for (ExecutorChunk ec : useableChunks) {
                        if (ec.node != null && availableNames.contains(ec.node.getNodeName())) {
                            anyStillAvailable = true;
                            break;
                        }
                    }
                    if (anyStillAvailable) {
                        chunksForThisRound = filterToAvailableNodesThisRound(useableChunks, availableNames);
                        LOGGER.log(FINER, tracePrefix(trace_id) + "Least load balancer: immediate assign for label-restricted work (using matching nodes still available this round)");
                    } else {
                        LOGGER.log(FINER, tracePrefix(trace_id) + "Least load balancer: all useableChunks are on already-assigned nodes; skip immediate assign to avoid double-assign");
                    }
                }
                if (chunksForThisRound.isEmpty()) {
                    logMappingFailureDiagnostics(ws, useableChunks.size(), true, trace_id);
                    logMapDuration(startNanos, "null (no chunks for this round)", trace_id);
                    return null;
                }

                Mapping m = ws.new Mapping();
                if (assignGreedily(m, chunksForThisRound, 0)) {
                    assert m.isCompletelyValid();
                    markNodesUsed(m);
                    logMapDuration(startNanos, "mapped", trace_id);
                    return m;
                } else {
                    logMappingFailureDiagnostics(ws, useableChunks.size(), false, trace_id);
                    LOGGER.log(FINE, tracePrefix(trace_id) + "Least load balancer was unable to define mapping. chunksForThisRound={0}", chunksForThisRound.size());
                    logMapDuration(startNanos, "null (assignGreedily failed)", trace_id);
                    return null;
                }

            } else {
                Mapping result = getFallBackLoadBalancer().map(task, ws);
                logMapDuration(startNanos, "fallback -> " + (result != null ? "mapped" : "null"), trace_id);
                return result;
            }

        } catch (Exception e) {
            LOGGER.log(WARNING, tracePrefix(trace_id) + "Least load balancer failed", e);
            logMapDuration(startNanos, "null (exception)", trace_id);
            return null;
        }
    }

    /**
     * Log elapsed time for the map() call (benchmark).
     */
    private void logMapDuration(long startNanos, String outcome, String traceId) {
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        LOGGER.log(FINE, tracePrefix(traceId) + "LeastLoadBalancer.map() finished in {0} ms, outcome: {1}", new Object[]{elapsedMs, outcome});
    }

    /**
     * When mapping fails, log why: worksheet size, per-work applicable vs available counts.
     * @param whenUseableZero if true, useableChunks was 0 (no applicable+available chunk in worksheet)
     */
    private void logMappingFailureDiagnostics(MappingWorksheet ws, int useableChunksSize, boolean whenUseableZero, String traceId) {
        int worksheetExecutors = ws.executors.size();

        if (worksheetExecutors == 0) {
            Label assignedLabel = ws.item.getAssignedLabel();
            LOGGER.log(FINEST,
                    tracePrefix(traceId) + "Least load balancer received zero executor candidates from the Queue (worksheetExecutors=0). " +
                            "The Queue rejected every idle executor for this task before calling the balancer. " +
                            "Task: {0}, assignedLabel: {1}. Check that some node has this label and can take the task (node.canTake / permissions).",
                    new Object[]{ws.item.task.getFullDisplayName(), assignedLabel != null ? assignedLabel.getDisplayName() : "none"});
            return;
        }

        StringBuilder workDetail = new StringBuilder();
        for (int i = 0; i < ws.works.size(); i++) {
            List<ExecutorChunk> applicable = ws.works(i).applicableExecutorChunks();
            int available = 0;
            for (ExecutorChunk ec : applicable) {
                if (isAvailable(ec)) available++;
            }
            if (workDetail.length() > 0) workDetail.append(" ");
            workDetail.append("work").append(i).append("Applicable=").append(applicable.size())
                    .append(" work").append(i).append("Available=").append(available);
        }
        if (whenUseableZero) {
            Set<String> forFilter = getNodeNamesForFilter(ws);
            LOGGER.log(FINE, tracePrefix(traceId) + "Least load balancer was unable to define mapping. works={0} useableChunks=0 availableNodesThisRound={1} nodesForLabel={2} worksheetExecutors={3} ({4})",
                    new Object[]{ws.works.size(), availableNodesThisRound.size(), forFilter.size(), worksheetExecutors, workDetail});
        } else {
            LOGGER.log(FINE, tracePrefix(traceId) + "Least load balancer was unable to define mapping. works={0} useableChunks={1} worksheetExecutors={2} ({3})",
                    new Object[]{ws.works.size(), useableChunksSize, worksheetExecutors, workDetail});
        }
    }

    /**
     * Re-populate the set of nodes considered available this round from Jenkins.
     * Includes only computers that are online, accepting tasks, and have at least one idle executor.
     * Populates both the global set and per-label sets so high-demand labels (e.g. x86_64_medium) have
     * their own capacity and are not starved by a single global round.
     */
    private void refreshAvailableNodes(String traceId) {
        availableNodesThisRound.clear();
        availableNodesThisRoundByLabel.clear();

        Jenkins jenkins = Jenkins.get();
        for (Computer c : jenkins.getComputers()) {
            Node node = c.getNode();
            if (node == null || c.isOffline() || !c.isAcceptingTasks() || c.countIdle() <= 0) {
                continue;
            }
            String nodeName = node.getNodeName();
            availableNodesThisRound.put(nodeName, node);

            for (Label label : jenkins.getLabels()) {
                if (label.contains(node)) {
                    availableNodesThisRoundByLabel
                            .computeIfAbsent(label.getDisplayName(), k -> new HashSet<>())
                            .add(nodeName);
                }
            }
        }
        LOGGER.log(FINE, tracePrefix(traceId) + "Least load balancer refreshed available nodes: {0} total, {1} labels",
                new Object[]{availableNodesThisRound.size(), availableNodesThisRoundByLabel.size()});
    }

    /**
     * Return the set of node names to use for "available this round" filtering. When the task has a single
     * work with an assigned label, use that label's set so each label has its own capacity; otherwise use global.
     */
    private Set<String> getNodeNamesForFilter(MappingWorksheet ws) {
        if (ws.works.size() == 1) {
            Label assigned = ws.works(0).assignedLabel;
            if (assigned != null) {
                Set<String> forLabel = availableNodesThisRoundByLabel.get(assigned.getDisplayName());
                if (forLabel != null && !forLabel.isEmpty()) {
                    return forLabel;
                }
            }
        }
        return availableNodesThisRound.keySet();
    }

    /**
     * Filter chunks to only those whose node is in the given available set (not yet used this round).
     */
    private List<ExecutorChunk> filterToAvailableNodesThisRound(List<ExecutorChunk> chunks, Set<String> availableNodeNames) {
        List<ExecutorChunk> out = new ArrayList<>(chunks.size());
        for (ExecutorChunk ec : chunks) {
            if (ec.node != null && availableNodeNames.contains(ec.node.getNodeName())) {
                out.add(ec);
            }
        }
        return out;
    }

    /**
     * Mark each node assigned in the mapping as used this round (remove from global and all per-label sets).
     */
    private void markNodesUsed(Mapping m) {
        for (int i = 0; i < m.size(); i++) {
            ExecutorChunk ec = m.assigned(i);
            if (ec != null && ec.node != null) {
                String nodeName = ec.node.getNodeName();
                availableNodesThisRound.remove(nodeName);
                for (Set<String> labelSet : availableNodesThisRoundByLabel.values()) {
                    labelSet.remove(nodeName);
                }
            }
        }
    }

    /**
     * True if any work chunk in the worksheet has an assigned label (label-restricted work).
     */
    private static boolean isLabelRestrictedWork(MappingWorksheet ws) {
        for (int i = 0; i < ws.works.size(); i++) {
            WorkChunk w = ws.works(i);
            if (w.assignedLabel != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * True if this task has a single work with an assigned label and that label's "available this round" set is empty.
     * Used to refresh proactively so we pick up newly provisioned nodes for that label without waiting for the retry path.
     */
    private boolean isLabelSetEmptyForTask(MappingWorksheet ws) {
        if (ws.works.size() != 1) {
            return false;
        }
        Label assigned = ws.works(0).assignedLabel;
        if (assigned == null) {
            return false;
        }
        Set<String> forLabel = availableNodesThisRoundByLabel.get(assigned.getDisplayName());
        return forLabel == null || forLabel.isEmpty();
    }

    /**
     * Extract a list of applicable {@link ExecutorChunk}s sorted in least loaded order.
     * Only considers chunks that are clearly available (node non-null, not offline).
     *
     * @param ws - The mapping worksheet
     * @return - A list of ExecutorChunk in least loaded order
     */
    private List<ExecutorChunk> getApplicableSortedByLoad(MappingWorksheet ws) {

        List<ExecutorChunk> chunks = new ArrayList<>();
        for (int i = 0; i < ws.works.size(); i++) {
            for (ExecutorChunk ec : ws.works(i).applicableExecutorChunks()) {
                if (isAvailable(ec)) {
                    chunks.add(ec);
                }
            }
        }
        if (ws.works.size() > 1) {
            Collections.shuffle(chunks); // See JENKINS-18323
        }
        chunks.sort(EXECUTOR_CHUNK_COMPARATOR);
        return chunks;

    }

    /**
     * True if this executor chunk is clearly available: node is set and
     * computer is not offline and accepting tasks.
     */
    private static boolean isAvailable(ExecutorChunk ec) {
        if (ec.node == null) {
            return false;
        }
        return !ec.computer.isOffline() && ec.computer.isAcceptingTasks();
    }

    private boolean isDisabled(Task task) {

        SubTask subTask = task.getOwnerTask();

        if (subTask instanceof Job) {
            Job<?, ?> job = (Job<?, ?>) subTask;
            LeastLoadDisabledProperty property = job.getProperty(LeastLoadDisabledProperty.class);
            // If the job configuration hasn't been saved after installing the plugin, the property will be null. Assume
            // that the user wants to enable functionality by default.
            if (property != null) {
                return property.isLeastLoadDisabled();
            }
            return false;
        } else {
            return true;
        }

    }

    private boolean assignGreedily(Mapping m, List<ExecutorChunk> executors, int i) {

        // fully assigned
        if (m.size() == i) {
            return true;
        }

        for (ExecutorChunk ec : executors) {
            // let's attempt this assignment
            m.assign(i, ec);
            if (m.isPartiallyValid() && assignGreedily(m, executors, i + 1)) {
                // successful greedily allocation
                return true;
            }

            // otherwise 'ec' wasn't a good fit for us. try next.
        }

        // every attempt failed
        m.assign(i, null);
        return false;

    }

    /**
     * Retrieves the fallback {@link LoadBalancer}
     *
     * @return - fallback {@link LoadBalancer}
     */
    public LoadBalancer getFallBackLoadBalancer() {
        return fallback;
    }

    /**
     * Natural order: idle chunks are "greater" than non-idle; among same idle status, more idle executors are "greater".
     * Used with {@link Collections#reverseOrder(Comparator)} as {@link #EXECUTOR_CHUNK_COMPARATOR}, so after sort
     * idle chunks come first (lower index), then by descending countIdle — i.e. least loaded first.
     */
    protected static class ExecutorChunkComparator implements Comparator<ExecutorChunk>, Serializable {
        private static final long serialVersionUID = 1L;

        public int compare(ExecutorChunk ec1, ExecutorChunk ec2) {

            if (ec1 == ec2) {
                return 0;
            }

            Computer com1 = ec1.computer;
            Computer com2 = ec2.computer;

            if (isIdle(com1) && !isIdle(com2)) {
                return 1;
            } else if (isIdle(com2) && !isIdle(com1)) {
                return -1;
            } else {
                return Integer.compare(com1.countIdle(), com2.countIdle());
            }

        }

        // Can't use computer.isIdle() as it can return false when assigned
        // a multi-configuration job even though no executors are being used
        private boolean isIdle(Computer computer) {
            return computer.countBusy() == 0;
        }

    }
}
