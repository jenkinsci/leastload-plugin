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
import java.util.Date;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.WARNING;

/**
 * This strategy chooses {@link Executor}s by preferring machines that have been left unused the longest,
 * regardless of how many {@link Executor}s are available on the machine. Assigning work to a machine
 * will send it to the back of the "queue"; this strategy is essentially a round-robin strategy.
 */
public class LongestUnusedMachineStrategy {
    private final Comparator<ExecutorChunk> EXECUTOR_CHUNK_COMPARATOR = new ExecutorChunkComparator();
    private final Map<String, Date> lastAssignedTimes = new HashMap<String, Date>();

    public LongestUnusedMachineStrategy() {
    }

    public Mapping map(Task task, MappingWorksheet ws) {
        List<ExecutorChunk> useableChunks = getApplicableSortedByLoad(ws);
        Mapping m = ws.new Mapping();
        if (assignGreedily(m, useableChunks, 0)) {
            assert m.isCompletelyValid();
            return m;
        } else {
            return null;
        }
    }

    public void save(Mapping mapping) {
        // Track last assigned times, using the current time and the name (string) of the
        // computer the executor is attached to.
        //
        // Relying on name can be problematic, e.g., Swarm Clients where the network interfaces
        // change on reboot (like MacOS). However, it is safer than holding onto a reference
        // to the Computer itself -- over the lifetime of the Jenkins server, we might see
        // hundreds of thousands of Computers come and go.
        //
        // A future improvement might be to run through the lastAssignedTimes hash and clear
        // any entries where the value is older than some constant (perhaps 48 hours).
        //
        if (mapping != null) {
            Date now = new Date();
            for (int i = 0; i < mapping.size(); i++) {
                lastAssignedTimes.put(mapping.assigned(i).computer.getName(), now);
            }
        }
    }

    /**
     * Extract a list of applicable {@link ExecutorChunk}s sorted in least loaded order
     *
     * @param ws - The mapping worksheet
     * @return -A list of ExecutorChunk in least loaded order
     */
    private List<ExecutorChunk> getApplicableSortedByLoad(MappingWorksheet ws) {
        List<ExecutorChunk> chunks = new ArrayList<ExecutorChunk>();
        for (int i = 0; i < ws.works.size(); i++) {
            for (ExecutorChunk ec : ws.works(i).applicableExecutorChunks()) {
                chunks.add(ec);
            }
        }
        Collections.shuffle(chunks); // See JENKINS-18323
        Collections.sort(chunks, EXECUTOR_CHUNK_COMPARATOR);
        return chunks;
    }

    private boolean assignGreedily(Mapping m, List<ExecutorChunk> executors, int i) {
        if (m.size() == i) {
            return true;
        }

        for (ExecutorChunk ec : executors) {
            m.assign(i, ec);
            if (m.isPartiallyValid() && assignGreedily(m, executors, i + 1)) {
                return true;
            }
        }

        m.assign(i, null);
        return false;
    }

    protected class ExecutorChunkComparator implements Comparator<ExecutorChunk> {
        public int compare(ExecutorChunk ec1, ExecutorChunk ec2) {
            if (ec1 == ec2) {
                return 0;
            }

            Date used1 = lastAssignedTimes.get(ec1.computer.getName());
            Date used2 = lastAssignedTimes.get(ec2.computer.getName());

            // Computers we've never assigned a task come first, otherwise they'd never get into the
            // sorted rotation.
            if (used1 == null) return -1;
            if (used2 == null) return 1;

            if (used1.before(used2)) return -1;
            if (used2.before(used1)) return 1;
            return 0;
        }
    }
}
