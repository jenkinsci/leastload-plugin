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
import hudson.init.Initializer;
import hudson.model.Computer;
import hudson.model.Executor;
import hudson.model.Job;
import hudson.model.LoadBalancer;
import hudson.model.Queue.Task;
import hudson.model.queue.MappingWorksheet;
import hudson.model.queue.MappingWorksheet.ExecutorChunk;
import hudson.model.queue.MappingWorksheet.Mapping;
import hudson.model.queue.SubTask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import jenkins.model.Jenkins;

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINER;
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
 *
 * @author brendan.nolan@gmail.com
 */
public class LeastLoadBalancer extends LoadBalancer {

    private static final Logger LOGGER = Logger.getLogger(LeastLoadBalancer.class.getName());

    private static final Comparator<ExecutorChunk> EXECUTOR_CHUNK_COMPARATOR = Collections.reverseOrder(new ExecutorChunkComparator());

    private final LoadBalancer fallback;

    /**
     * Node names that are considered available for assignment this round (not yet used).
     * When empty, we refresh from Jenkins (nodes that are online, accepting tasks, with idle executors).
     * Access only under Queue lock (map() is serialized by the caller).
     */
    private final Set<String> availableNodeNamesThisRound = new HashSet<>();

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

    @Override
    @CheckForNull
    public Mapping map(@NonNull Task task, MappingWorksheet ws) {

        try {

            if (!isDisabled(task)) {

                if (availableNodeNamesThisRound.isEmpty()) {
                    refreshAvailableNodes();
                }

                List<ExecutorChunk> useableChunks = getApplicableSortedByLoad(ws);
                List<ExecutorChunk> chunksForThisRound = filterToAvailableNodesThisRound(useableChunks);

                if (chunksForThisRound.isEmpty()) {
                    refreshAvailableNodes();
                    chunksForThisRound = filterToAvailableNodesThisRound(useableChunks);
                }
                if (chunksForThisRound.isEmpty()) {
                    logMappingFailureDiagnostics(ws, useableChunks.size(), true);
                    return null;
                }

                Mapping m = ws.new Mapping();
                if (assignGreedily(m, chunksForThisRound, 0)) {
                    assert m.isCompletelyValid();
                    markNodesUsed(m);
                    return m;
                } else {
                    logMappingFailureDiagnostics(ws, useableChunks.size(), false);
                    LOGGER.log(FINE, "Least load balancer was unable to define mapping. chunksForThisRound={0}", chunksForThisRound.size());
                    return null;
                }

            } else {
                return getFallBackLoadBalancer().map(task, ws);
            }

        } catch (Exception e) {
            LOGGER.log(WARNING, "Least load balancer failed", e);
            return null;
        }
    }

    /**
     * When mapping fails, log why: worksheet size, per-work applicable vs available counts.
     * @param whenUseableZero if true, useableChunks was 0 (no applicable+available chunk in worksheet)
     */
    private void logMappingFailureDiagnostics(MappingWorksheet ws, int useableChunksSize, boolean whenUseableZero) {
        int worksheetExecutors = ws.executors.size();
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
            LOGGER.log(FINE, "Least load balancer was unable to define mapping. works={0} useableChunks=0 availableNodesThisRound={1} worksheetExecutors={2} ({3})",
                    new Object[]{ws.works.size(), availableNodeNamesThisRound.size(), worksheetExecutors, workDetail});
        } else {
            LOGGER.log(FINE, "Least load balancer was unable to define mapping. works={0} useableChunks={1} worksheetExecutors={2} ({3})",
                    new Object[]{ws.works.size(), useableChunksSize, worksheetExecutors, workDetail});
        }
    }

    /**
     * Re-populate the set of node names considered available this round from Jenkins.
     * Includes only computers that are online, accepting tasks, and have at least one idle executor.
     */
    private void refreshAvailableNodes() {
        availableNodeNamesThisRound.clear();
        Jenkins j = Jenkins.get();
        if (j == null) {
            return;
        }
        for (Computer c : j.getComputers()) {
            if (c.getNode() == null || c.isOffline() || !c.isAcceptingTasks() || c.countIdle() <= 0) {
                continue;
            }
            availableNodeNamesThisRound.add(c.getNode().getNodeName());
        }
        LOGGER.log(FINER, "Least load balancer refreshed available nodes: {0} nodes", availableNodeNamesThisRound.size());
    }

    /**
     * Filter chunks to only those whose node is in the current round's available set (not yet used).
     */
    private List<ExecutorChunk> filterToAvailableNodesThisRound(List<ExecutorChunk> chunks) {
        List<ExecutorChunk> out = new ArrayList<>(chunks.size());
        for (ExecutorChunk ec : chunks) {
            if (ec.node != null && availableNodeNamesThisRound.contains(ec.node.getNodeName())) {
                out.add(ec);
            }
        }
        return out;
    }

    /**
     * Mark each node assigned in the mapping as used this round (remove from available set).
     */
    private void markNodesUsed(Mapping m) {
        for (int i = 0; i < m.size(); i++) {
            ExecutorChunk ec = m.assigned(i);
            if (ec != null && ec.node != null) {
                availableNodeNamesThisRound.remove(ec.node.getNodeName());
            }
        }
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
        Collections.shuffle(chunks); // See JENKINS-18323
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
                return com1.countIdle() - com2.countIdle();
            }

        }

        // Can't use computer.isIdle() as it can return false when assigned
        // a multi-configuration job even though no executors are being used
        private boolean isIdle(Computer computer) {
            return computer.countBusy() == 0;
        }

    }
}
