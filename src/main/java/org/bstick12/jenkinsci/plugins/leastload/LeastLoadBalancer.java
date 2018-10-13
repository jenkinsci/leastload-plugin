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
 * Jenkins {@link LoadBalancer}.
 * <p>The {@link LeastLoadBalancer} chooses {@link Executor}s that have the least load. An {@link Executor} is defined
 * as having the least load if it is idle or has the most available {@link Executor}s.
 * <p>If for any reason we are unsuccessful in creating a {@link Mapping} we fall back on the default Jenkins
 * {@link LoadBalancer#CONSISTENT_HASH} and try to use that.
 *
 * @author brendan.nolan@gmail.com
 */
public class LeastLoadBalancer extends LoadBalancer {
    private static final Logger LOGGER = Logger.getLogger(LeastLoadBalancer.class.getCanonicalName());

    private final LoadBalancer fallback;
    private final MostIdleExecutorsStrategy mostIdleExecutors;
    private final LongestUnusedMachineStrategy longestUnusedMachine;

    public enum StrategySelection {
        UseGlobal("Use Global", false),
        MostIdleExecutors("Prefer Idle", true),
        LongestUnusedMachine("Round Robin", true),
        Disabled("Disabled", true);

        private String displayName;
        private boolean global;

        StrategySelection(String displayName, boolean global) {
            this.displayName = displayName;
            this.global = global;
        }

        public String getDisplayName() {
            return displayName;
        }

        public boolean validGlobalOption() {
            return global;
        }
    }

    /**
     * Create the {@link LeastLoadBalancer} with a fallback that will be
     * used in case of any failures.
     *
     * @param fallback The LoadBalancer fallback to use in case of failure
     */
    public LeastLoadBalancer(LoadBalancer fallback) {
        Preconditions.checkNotNull(fallback, "You must provide a fallback implementation of the LoadBalancer");
        this.fallback = fallback;

        // Construct an instance of each supported mapping strategy
        this.mostIdleExecutors = new MostIdleExecutorsStrategy();
        this.longestUnusedMachine = new LongestUnusedMachineStrategy();
    }

    @Override
    public Mapping map(Task task, MappingWorksheet ws) {
        StrategySelection strategy = getStrategyForTask(task);

        if (strategy == StrategySelection.Disabled) {
            return save(getFallBackLoadBalancer().map(task, ws));
        }

        try {
            Mapping mapping;

            if (strategy == StrategySelection.MostIdleExecutors) {
                mapping = mostIdleExecutors.map(task, ws);
            } else if (strategy == StrategySelection.LongestUnusedMachine) {
                mapping = longestUnusedMachine.map(task, ws);
            } else {
                // Execution should never reach this point.
                throw new Exception("Least Load Balancer: unexpected strategy selection detected");
            }

            if (mapping == null) {
                LOGGER.log(FINE, "Least load balancer was unable to define mapping. Using fallback.");
                return save(getFallBackLoadBalancer().map(task, ws));
            } else {
                return mapping;
            }
        } catch (Exception e) {
            LOGGER.log(WARNING, "Least load balancer failed. Using fallback.", e);
            return save(getFallBackLoadBalancer().map(task, ws));
        }
    }

    /**
     * Retrieves the fallback {@link LoadBalancer}
     *
     * @return - fallback LoadBalancer
     */
    public LoadBalancer getFallBackLoadBalancer() {
        return fallback;
    }

    /**
     * Notify each supported strategy of the final mapping results, if applicable.
     *
     * @param m - the produced final mapping
     * @return - the same mapping
     */
    private Mapping save(Mapping mapping) {
        // Even if we aren't using a strategy for this particular mapping, allow each
        // strategy to record it, in case this is necessary for that strategy to behave
        // correctly the next time it maps to that executor.
        this.mostIdleExecutors.save(mapping);
        this.longestUnusedMachine.save(mapping);

        return mapping;
    }

    private StrategySelection getStrategyForTask(Task task) {
        StrategySelection strategy;
        SubTask subTask = task.getOwnerTask();

        if (subTask instanceof Job) {
            Job job = (Job)subTask;
            LeastLoadJobConfig config = (LeastLoadJobConfig)job.getProperty(LeastLoadJobConfig.class);
            // If the property is null, then this job hasn't been saved
            // after installing the plugin.
            if (config != null) {
                strategy = config.getStrategy();
            } else {
                strategy = StrategySelection.UseGlobal;
            }

            if (strategy == StrategySelection.UseGlobal) {
                strategy = LeastLoadGlobalConfig.get().getStrategy();

                // If global config is null, then the server config hasn't been saved
                // after installing the plugin. The default behavior of the plugin
                // is to use MostIdleExecutors.
                if (strategy == null) {
                    strategy = StrategySelection.MostIdleExecutors;
                }
            }
        } else {
            // For tasks that are not jobs, use Jenkins default load balancer.
            strategy = StrategySelection.Disabled;
        }

        return strategy;
    }
}
