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
 * This strategy chooses {@link Executor}s that have the least load. An {@link Executor} is defined
 * as having the least load if it is idle, or has the most available {@link Executor}s.
 *
 * @author brendan.nolan@gmail.com
 */
public class MostIdleExecutorsStrategy {
    private static final Comparator<ExecutorChunk> EXECUTOR_CHUNK_COMPARATOR = Collections.reverseOrder(new ExecutorChunkComparator());

    public MostIdleExecutorsStrategy() {
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
        // No-op
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
            return computer.countExecutors() - computer.countIdle() == 0 ? true : false;
        }
    }
}
