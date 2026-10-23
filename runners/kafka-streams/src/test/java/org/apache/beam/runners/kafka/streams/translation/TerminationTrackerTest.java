/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.kafka.streams.translation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TerminationTracker}. */
@RunWith(JUnit4.class)
public class TerminationTrackerTest {

  private final AtomicInteger calls = new AtomicInteger();

  /** A tracker for a topology that has finished starting up. */
  private TerminationTracker tracker() {
    TerminationTracker tracker = new TerminationTracker();
    tracker.onAllTerminated(calls::incrementAndGet);
    tracker.started();
    return tracker;
  }

  @Test
  public void doesNotFireBeforeTheTopologyHasFinishedStarting() {
    // Processors register as their task is initialized, so mid-startup the registered set is only
    // part of the pipeline. A source that drains that quickly would otherwise look like a finished
    // pipeline, and stopping there discards every stage that had not started yet — which shows up
    // as a run that reports success and produces no output.
    TerminationTracker tracker = new TerminationTracker();
    tracker.onAllTerminated(calls::incrementAndGet);

    tracker.register("source#0_0");
    tracker.terminate("source#0_0");
    assertThat("the rest of the topology may not exist yet", calls.get(), is(0));

    // The stage downstream of the repartition topic comes up late and has real work to do.
    tracker.register("downstream#1_0");
    tracker.started();
    assertThat(calls.get(), is(0));

    tracker.terminate("downstream#1_0");
    assertThat(calls.get(), is(1));
  }

  @Test
  public void firesOnStartupIfEverythingAlreadyTerminated() {
    // A pipeline short enough to drain entirely during startup still has to be noticed.
    TerminationTracker tracker = new TerminationTracker();
    tracker.onAllTerminated(calls::incrementAndGet);
    tracker.register("source#0_0");
    tracker.terminate("source#0_0");

    tracker.started();

    assertThat(calls.get(), is(1));
  }

  @Test
  public void firesOnceEveryRegisteredProcessorHasTerminated() {
    TerminationTracker tracker = tracker();
    tracker.register("stage#0_0");
    tracker.register("stage#0_1");

    tracker.terminate("stage#0_0");
    assertThat("one of two done is not the whole pipeline", calls.get(), is(0));

    tracker.terminate("stage#0_1");
    assertThat(calls.get(), is(1));
  }

  @Test
  public void doesNotFireWhileAProcessorIsStillRunning() {
    TerminationTracker tracker = tracker();
    // The shape that makes counting every processor necessary: one instance owning both sides of a
    // repartition topic. The upstream goes terminal as soon as it has written to the topic, while
    // the downstream still has to consume it.
    tracker.register("upstream#0_0");
    tracker.register("downstream#1_0");

    tracker.terminate("upstream#0_0");

    assertThat("stopping here would cut the downstream off", calls.get(), is(0));
  }

  @Test
  public void doesNotFireWithNothingRegistered() {
    TerminationTracker tracker = tracker();
    tracker.terminate("never-registered#0_0");
    assertThat(calls.get(), is(0));
  }

  @Test
  public void firesOnlyOnce() {
    TerminationTracker tracker = tracker();
    tracker.register("stage#0_0");

    tracker.terminate("stage#0_0");
    tracker.terminate("stage#0_0");

    assertThat(calls.get(), is(1));
  }

  @Test
  public void shuttingDownDoesNotFireAgain() {
    // What the callback does is stop the client, which closes every task's processors, and each of
    // those unregisters on the way out. So the tracker is asked again several times after the
    // pipeline has already been declared finished.
    TerminationTracker tracker = tracker();
    tracker.register("source#0_0");
    tracker.register("stage#1_0");
    tracker.terminate("source#0_0");
    tracker.terminate("stage#1_0");
    assertThat(calls.get(), is(1));

    tracker.unregister("source#0_0");
    tracker.unregister("stage#1_0");

    assertThat("stopping the pipeline must not stop it a second time", calls.get(), is(1));
  }

  @Test
  public void aProcessorThatMigratesAwayIsNoLongerWaitedOn() {
    TerminationTracker tracker = tracker();
    tracker.register("stage#0_0");
    tracker.register("stage#0_1");
    tracker.terminate("stage#0_0");

    // Task 0_1 is reassigned to another instance during a rebalance. What is left here is done, so
    // this instance has nothing to keep it alive.
    tracker.unregister("stage#0_1");

    assertThat(calls.get(), is(1));
  }

  @Test
  public void unregisteringTheLastProcessorDoesNotFire() {
    TerminationTracker tracker = tracker();
    tracker.register("stage#0_0");

    tracker.unregister("stage#0_0");

    assertThat("nothing registered means nothing finished", calls.get(), is(0));
  }

  @Test
  public void withoutACallbackNothingHappens() {
    TerminationTracker tracker = new TerminationTracker();
    tracker.register("stage#0_0");
    tracker.terminate("stage#0_0");
    // No callback set: the point is that this does not throw.
    assertThat(calls.get(), is(0));
  }
}
