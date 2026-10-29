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
package org.apache.beam.sdk.extensions.openlineage;

import io.openlineage.client.OpenLineage;
import org.apache.beam.sdk.PipelineResult;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches a running pipeline and emits periodic RUNNING events, mirroring the Flink integration's
 * {@code OpenLineageContinousJobTracker}: a daemon thread wakes up every {@code
 * trackingIntervalInSeconds} (default 60), sweeps any lineage reported through Beam metrics by the
 * IO connectors, and emits a RUNNING event. When it observes a terminal state it emits the terminal
 * event (DONE and UPDATED map to COMPLETE, CANCELLED to ABORT, other terminal states to FAIL) and
 * stops. The deterministic terminal trigger is {@link OpenLineagePipelineResult}; this tracker
 * covers callers that never invoke waitUntilFinish (e.g. detached submission), in which case a
 * runner whose {@code getState()} is unsupported may only ever produce RUNNING heartbeats.
 */
class OpenLineageJobTracker {

  private static final Logger LOG = LoggerFactory.getLogger(OpenLineageJobTracker.class);

  private final OpenLineageContext context;
  private final PipelineResult result;
  private final long intervalMillis;
  private @Nullable Thread trackingThread;

  OpenLineageJobTracker(OpenLineageContext context, PipelineResult result, int intervalSeconds) {
    this.context = context;
    this.result = result;
    this.intervalMillis = intervalSeconds * 1000L;
  }

  /** Starts the tracking thread; returns immediately. */
  void startTracking() {
    Thread thread =
        new Thread(
            () -> {
              LOG.info("Starting OpenLineage job tracking every {} ms", intervalMillis);
              while (true) {
                context.sweepLineageMetrics(result);
                PipelineResult.State state = currentState();
                if (state != null && state.isTerminal()) {
                  context.onJobFinished(terminalEventType(state), null);
                  return;
                }
                context.onTrackingTick();
                try {
                  Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
              }
            },
            "beam-openlineage-job-tracker");
    thread.setDaemon(true);
    thread.start();
    this.trackingThread = thread;
  }

  void stopTracking() {
    Thread thread = trackingThread;
    if (thread != null) {
      thread.interrupt();
    }
  }

  private PipelineResult.@Nullable State currentState() {
    try {
      return result.getState();
    } catch (RuntimeException e) {
      LOG.debug("Unable to poll pipeline state", e);
      return null;
    }
  }

  static OpenLineage.RunEvent.EventType terminalEventType(PipelineResult.State state) {
    switch (state) {
      case DONE:
      case UPDATED:
        // UPDATED means the job was replaced by a newer version (e.g. Dataflow --update); the
        // run itself ended successfully.
        return OpenLineage.RunEvent.EventType.COMPLETE;
      case CANCELLED:
        return OpenLineage.RunEvent.EventType.ABORT;
      default:
        return OpenLineage.RunEvent.EventType.FAIL;
    }
  }
}
