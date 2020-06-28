package org.bstick12.jenkinsci.plugins.leastload;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import hudson.Launcher;
import hudson.model.AbstractBuild;
import hudson.model.BuildListener;
import hudson.model.FreeStyleBuild;
import hudson.model.FreeStyleProject;
import hudson.slaves.DumbSlave;
import hudson.slaves.RetentionStrategy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.jvnet.hudson.test.JenkinsRule;
import org.jvnet.hudson.test.TestBuilder;

import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LeastLoadIntegrationTest {

    @Rule public JenkinsRule j = new JenkinsRule();

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void before() throws Exception {
        new LeastLoadPlugin().start();
    }

    @Test
    public void smokes() throws Exception {
        DumbSlave agent1 =
                new DumbSlave(
                        "agent0",
                        temporaryFolder.newFolder().getPath(),
                        j.createComputerLauncher(null));
        agent1.setLabelString("leastload");
        agent1.setNumExecutors(4);
        agent1.setRetentionStrategy(RetentionStrategy.NOOP);
        j.jenkins.addNode(agent1);
        j.waitOnline(agent1);

        DumbSlave agent2 =
                new DumbSlave(
                        "agent1",
                        temporaryFolder.newFolder().getPath(),
                        j.createComputerLauncher(null));
        agent2.setLabelString("leastload");
        agent2.setNumExecutors(4);
        agent2.setRetentionStrategy(RetentionStrategy.NOOP);
        j.jenkins.addNode(agent2);
        j.waitOnline(agent2);

        FreeStyleProject p = j.createFreeStyleProject();
        p.setAssignedLabel(j.jenkins.getLabel("leastload"));
        p.setConcurrentBuild(true);
        AtomicInteger atomicResult =
                new AtomicInteger(0); // used as a reference passed to the build step
        Semaphore semaphore = new Semaphore(0);
        p.getBuildersList().add(new SemaphoredBuilder(semaphore, atomicResult));

        // At the beginning, both agents should be idle.
        assertTrue(agent1.toComputer().isIdle());
        assertTrue(agent2.toComputer().isIdle());

        // After starting the first build, one executor should be used.
        FreeStyleBuild b1 = p.scheduleBuild2(0).waitForStart();
        j.waitForMessage("will try to acquire one permit", b1);
        assertTrue(
                (agent1.toComputer().countBusy() == 0 && agent2.toComputer().countBusy() == 1)
                        || (agent1.toComputer().countBusy() == 1
                                && agent2.toComputer().countBusy() == 0));

        // After starting the second build, the load should be balanced across both agents.
        FreeStyleBuild b2 = p.scheduleBuild2(0).waitForStart();
        j.waitForMessage("will try to acquire one permit", b2);
        assertEquals(1, agent1.toComputer().countBusy());
        assertEquals(1, agent2.toComputer().countBusy());

        // After starting the third build, three executors should be used: two on one agent and one
        // on the other.
        FreeStyleBuild b3 = p.scheduleBuild2(0).waitForStart();
        j.waitForMessage("will try to acquire one permit", b3);
        assertTrue(
                (agent1.toComputer().countBusy() == 1 && agent2.toComputer().countBusy() == 2)
                        || (agent1.toComputer().countBusy() == 2
                                && agent2.toComputer().countBusy() == 1));

        // After starting the fourth build, four executors should be used: two on each agent.
        FreeStyleBuild b4 = p.scheduleBuild2(0).waitForStart();
        j.waitForMessage("will try to acquire one permit", b4);
        assertEquals(2, agent1.toComputer().countBusy());
        assertEquals(2, agent2.toComputer().countBusy());

        // Clean up
        semaphore.release(4);
        j.assertBuildStatusSuccess(j.waitForCompletion(b1));
        j.assertBuildStatusSuccess(j.waitForCompletion(b2));
        j.assertBuildStatusSuccess(j.waitForCompletion(b3));
        j.assertBuildStatusSuccess(j.waitForCompletion(b4));
        assertEquals(4, atomicResult.get());
    }

    public static class SemaphoredBuilder extends TestBuilder {
        private transient Semaphore semaphore;
        private transient AtomicInteger atomicInteger;

        SemaphoredBuilder(Semaphore semaphore, AtomicInteger atomicInteger) {
            this.semaphore = Objects.requireNonNull(semaphore);
            this.atomicInteger = Objects.requireNonNull(atomicInteger);
        }

        @Override
        public boolean perform(AbstractBuild<?, ?> build, Launcher launcher, BuildListener listener)
                throws InterruptedException {
            listener.getLogger().println("job started, will try to acquire one permit");
            if (semaphore.tryAcquire(120, TimeUnit.SECONDS)) {
                listener.getLogger().println("permit acquired");
                atomicInteger.incrementAndGet();
                return true;
            } else {
                return false;
            }
        }
    }
}
