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
 * <p>Kafka Streams has no notion of a finished processor; a topology runs until something closes
 * the client. A bounded pipeline does finish, and the runner already knows when, because every
 * processor emits {@link
 * org.apache.beam.sdk.transforms.windowing.BoundedWindow#TIMESTAMP_MAX_VALUE} once its input is
 * exhausted. This collects those reports and fires a callback when nothing is left to do.
 *
 * <p>No coordination between instances is needed: a watermark crossing a repartition topic is
 * broadcast to every partition (see {@link GroupByKeyBroadcastPartitioner}), so every task observes
 * the terminal watermark itself and all instances reach the same conclusion independently.
 *
 * <p>Every local processor is counted, not just the first. One instance can own tasks from both
 * sides of a repartition topic, and the upstream side goes terminal as soon as it has written to
 * the topic while the downstream side still has to consume it. Stopping at the first would drop
 * that work and still report success.
 *
 * <p>A tracker belongs to one pipeline rather than to the JVM: the job server runs many jobs in one
 * process, and a static tracker would let one job stop another. An unbounded pipeline never
 * produces a terminal watermark, so the callback never fires and the client keeps running.
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
