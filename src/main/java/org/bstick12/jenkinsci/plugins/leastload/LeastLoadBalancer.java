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
import java.util.List;
import java.util.logging.Logger;

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.WARNING;

/**
 * A {@link LoadBalancer} implementation that the leastload plugin uses to replace the default
 * Jenkins {@link LoadBalancer}
 * <p>The {@link LeastLoadBalancer} chooses {@link Executor}s that have the least load. An {@link Executor} is defined
 * as having the least load if it is idle or has the most available {@link Executor}s
 * <p>If for any reason we are unsuccessful in creating a {@link Mapping} we fall back on the default Jenkins
 * {@link LoadBalancer#CONSISTENT_HASH} and try to use that.
 *
 * @author brendan.nolan@gmail.com
 */
public class LeastLoadBalancer extends LoadBalancer {

    private static final Logger LOGGER = Logger.getLogger(LeastLoadBalancer.class.getCanonicalName());

    private static final Comparator<ExecutorChunk> EXECUTOR_CHUNK_COMPARATOR = Collections.reverseOrder(new ExecutorChunkComparator());

    private final LoadBalancer fallback;

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

    @Override
    public Mapping map(Task task, MappingWorksheet ws) {

        try {

            if (!isDisabled(task)) {

                List<ExecutorChunk> useableChunks = getApplicableSortedByLoad(ws);
                // do a greedy assignment
                Mapping m = ws.new Mapping();
                if (assignGreedily(m, useableChunks, 0)) {
                    assert m.isCompletelyValid();
                    return m;
                } else {
                    LOGGER.log(FINE, "Least load balancer was unable to define mapping. Falling back to double check");
                    return getFallBackLoadBalancer().map(task, ws);
                }

            } else {
                return getFallBackLoadBalancer().map(task, ws);
            }

        } catch (Exception e) {
            LOGGER.log(WARNING, "Least load balancer failed will use fallback", e);
            return getFallBackLoadBalancer().map(task, ws);
        }
    }

    /**
     * Extract a list of applicable {@link ExecutorChunk}s sorted in least loaded order
     *
     * @param ws - The mapping worksheet
     * @return -A list of ExecutorChunk in least loaded order
     */
    private List<ExecutorChunk> getApplicableSortedByLoad(MappingWorksheet ws) {

        List<ExecutorChunk> chunks = new ArrayList<>();
        for (int i = 0; i < ws.works.size(); i++) {
            chunks.addAll(ws.works(i).applicableExecutorChunks());
        }
        Collections.shuffle(chunks); // See JENKINS-18323
        chunks.sort(EXECUTOR_CHUNK_COMPARATOR);
        return chunks;

    }

    private boolean isDisabled(Task task) {

        SubTask subTask = task.getOwnerTask();

        if (subTask instanceof Job) {
            Job<?, ?> job = (Job<?, ?>) subTask;
            LeastLoadDisabledProperty property = (LeastLoadDisabledProperty) job.getProperty(LeastLoadDisabledProperty.class);
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
            return computer.countExecutors() - computer.countIdle() == 0;
        }

    }

}
