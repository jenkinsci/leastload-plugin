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

import hudson.Extension;
import hudson.model.Job;
import hudson.model.JobProperty;
import hudson.model.JobPropertyDescriptor;

import org.kohsuke.stapler.DataBoundConstructor;

/**
 * Per-job configuration for the {@link LeastLoadBalancer}, implemented as a
 * {@link JobProperty}.
 *
 * @author brendan.nolan@gmail.com
 */
public class LeastLoadJobConfig extends JobProperty<Job<?, ?>> {
    private final LeastLoadBalancer.StrategySelection strategy;

    @DataBoundConstructor
    public LeastLoadJobConfig(LeastLoadBalancer.StrategySelection strategy) {
        this.strategy = strategy;
    }

    public LeastLoadBalancer.StrategySelection getStrategy() {
        return strategy;
    }

    public boolean isOverriden() {
        return strategy != null && strategy != LeastLoadBalancer.StrategySelection.UseGlobal;
    }

    @Extension
    public static class DescriptorImpl extends JobPropertyDescriptor {
        @Override
        public String getDisplayName() {
            return "Least Load Strategy Property";
        }
    }
}
