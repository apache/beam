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

import java.util.HashSet;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides when a bounded pipeline has finished, so the Kafka Streams client can be stopped.
 *
 * <p>Kafka Streams has no notion of a processor being finished: a topology runs until something
 * closes the client. A bounded Beam pipeline does finish, though, and the runner already knows
 * when: every processor emits a watermark of {@link
 * org.apache.beam.sdk.transforms.windowing.BoundedWindow#TIMESTAMP_MAX_VALUE} once its input is
 * exhausted. This class collects those reports and fires a callback when there is nothing left to
 * do.
 *
 * <h3>Why no coordination between instances is needed</h3>
 *
 * <p>A watermark that crosses a repartition topic is broadcast to <em>every</em> partition (see
 * {@link GroupByKeyBroadcastPartitioner}), so every task of every downstream transform observes the
 * terminal watermark on its own, whichever instance it happens to run on. Each instance can
 * therefore decide to stop from what it sees locally, and they all reach the same conclusion
 * without talking to each other.
 *
 * <h3>Why every local processor has to be counted, not just the first</h3>
 *
 * <p>One instance can own tasks from both sides of a repartition topic. The upstream side goes
 * terminal as soon as it has written its data to the topic, while the downstream side still has to
 * consume it. Stopping the client when the first processor finishes would cut that downstream work
 * off and report the pipeline as done having silently dropped it. So the callback only fires once
 * every processor instance registered here has terminated.
 *
 * <p>An instance that happens to own only upstream tasks still terminates on its own, which is
 * correct: what it wrote is durable in the topic for whichever instance reads it.
 *
 * <h3>Scope</h3>
 *
 * <p>One tracker belongs to one pipeline, not to the JVM. The job server runs many jobs in a single
 * process, so a shared static tracker would let one pipeline finishing tear down another.
 *
 * <p>A pipeline with an unbounded source never produces a terminal watermark, so the callback never
 * fires and the client keeps running — which is the intended behaviour for a streaming job.
 */
public class TerminationTracker {

  private static final Logger LOG = LoggerFactory.getLogger(TerminationTracker.class);

  /** Processor instances currently running here, by {@code transformId#taskId}. */
  private final Set<String> live = new HashSet<>();

  /** Those of {@link #live} that have emitted the terminal watermark. */
  private final Set<String> terminated = new HashSet<>();

  /**
   * What to do when the pipeline is finished, cleared as it is taken.
   *
   * <p>Clearing it is what stops it running twice: a pipeline only finishes once, but processors go
   * on reporting afterwards — the callback stops the client, and closing it makes every remaining
   * task close its processors, each of which unregisters and asks again.
   */
  private @Nullable Runnable onAllTerminated;

  /**
   * Whether the topology is fully up, and so whether the registered processors are the whole set.
   *
   * <p>Processors register as their task is initialized, which happens gradually while the client
   * starts. Deciding before that is finished reads "every processor is done" off a set that is
   * merely incomplete: on a short pipeline the source can drain before the task downstream of the
   * repartition topic exists, and stopping there discards the rest of the pipeline and reports a
   * successful run that produced nothing.
   */
  private boolean started;

  /**
   * Sets what to do when the pipeline is finished. Must be called before the topology starts, so
   * that no processor can terminate before there is anything to call.
   *
   * <p>The callback runs on whichever thread completes the picture: usually the Kafka Streams task
   * thread reporting the last termination, but the thread reporting startup when the pipeline
   * drained before it finished starting. Both are threads {@code KafkaStreams.close()} waits for,
   * so stopping the client is the callback's job to hand off to a thread of its own.
   */
  public synchronized void onAllTerminated(Runnable callback) {
    this.onAllTerminated = callback;
  }

  /**
   * Marks the topology as fully started, after which the registered processors are taken to be the
   * whole set. Called when Kafka Streams reports {@code RUNNING}, which it does once every assigned
   * task has been initialized.
   *
   * <p>A pipeline short enough to drain during startup will already have reported terminations by
   * then, so this re-checks rather than only gating what comes later.
   */
  public void started() {
    Runnable callback;
    synchronized (this) {
      started = true;
      callback = takeCallbackIfDone();
    }
    run(callback);
  }

  /** Registers a processor instance, called from {@code Processor#init}. */
  synchronized void register(String instanceId) {
    live.add(instanceId);
  }

  /**
   * Removes a processor instance, called from {@code Processor#close}, so that a task migrating
   * away during a rebalance is not waited on forever.
   */
  void unregister(String instanceId) {
    Runnable callback;
    synchronized (this) {
      live.remove(instanceId);
      terminated.remove(instanceId);
      callback = takeCallbackIfDone();
    }
    run(callback);
  }

  /**
   * Records that a processor instance has emitted the terminal watermark and has no further work.
   */
  void terminate(String instanceId) {
    Runnable callback;
    synchronized (this) {
      if (!live.contains(instanceId)) {
        // Terminated after being unregistered, or never registered: nothing is waiting on it.
        return;
      }
      if (terminated.add(instanceId)) {
        LOG.debug(
            "Processor {} reached the terminal watermark ({}/{})",
            instanceId,
            terminated.size(),
            live.size());
      }
      callback = takeCallbackIfDone();
    }
    run(callback);
  }

  private static void run(@Nullable Runnable callback) {
    // Deliberately outside the lock: the callback shuts the pipeline down, and holding the monitor
    // while calling into shutdown makes this class part of that path for anyone who changes what
    // the callback does later.
    if (callback != null) {
      callback.run();
    }
  }

  /**
   * Returns the callback to run if the pipeline is finished, having claimed the right to run it.
   */
  private @Nullable Runnable takeCallbackIfDone() {
    if (!started || live.isEmpty() || !terminated.containsAll(live)) {
      return null;
    }
    Runnable callback = onAllTerminated;
    if (callback == null) {
      // Never set, or already taken — either way there is nothing left to do.
      return null;
    }
    onAllTerminated = null;
    LOG.info(
        "All {} processor instances reached the terminal watermark; stopping the pipeline",
        live.size());
    return callback;
  }
}
