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
package org.apache.beam.runners.fnexecution.control;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Utility methods for creating {@link BundleCheckpointHandler}s. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BundleCheckpointHandlers {

  /**
   * A {@link BundleCheckpointHandler} which uses {@link
   * org.apache.beam.runners.core.TimerInternals.TimerData} and {@link
   * org.apache.beam.sdk.state.ValueState} to reschedule {@link DelayedBundleApplication}.
   */
  public static class StateAndTimerBundleCheckpointHandler<T> implements BundleCheckpointHandler {

    private final TimerInternalsFactory<T> timerInternalsFactory;
    private final StateInternalsFactory<T> stateInternalsFactory;
    private final Coder<WindowedValue<T>> residualCoder;
    private final Coder windowCoder;
    private final IdGenerator idGenerator = IdGenerators.incrementingLongs();
    public static final String SDF_PREFIX = "sdf_checkpoint";

    public StateAndTimerBundleCheckpointHandler(
        TimerInternalsFactory<T> timerInternalsFactory,
        StateInternalsFactory<T> stateInternalsFactory,
        Coder<WindowedValue<T>> residualCoder,
        Coder windowCoder) {
      this.residualCoder = residualCoder;
      this.windowCoder = windowCoder;
      this.timerInternalsFactory = timerInternalsFactory;
      this.stateInternalsFactory = stateInternalsFactory;
    }

    /**
     * A helper function to help check whether the given timer is the timer which is set for
     * rescheduling {@link DelayedBundleApplication}.
     */
    public static boolean isSdfTimer(String timerId) {
      return timerId.startsWith(SDF_PREFIX);
    }

    private static String constructSdfCheckpointId(String id, int index) {
      return SDF_PREFIX + ":" + id + ":" + index;
    }

    @Override
    public void onCheckpoint(ProcessBundleResponse response) {
      String id = idGenerator.getId();
      for (int index = 0; index < response.getResidualRootsCount(); index++) {
        DelayedBundleApplication residual = response.getResidualRoots(index);
        if (!residual.hasApplication()) {
          continue;
        }
        String tag = constructSdfCheckpointId(id, index);
        try {
          WindowedValue<T> stateValue =
              CoderUtils.decodeFromByteArray(
                  residualCoder, residual.getApplication().getElement().toByteArray());
          TimerInternals timerInternals =
              timerInternalsFactory.timerInternalsForKey(stateValue.getValue());
          StateInternals stateInternals =
              stateInternalsFactory.stateInternalsForKey(stateValue.getValue());
          // Calculate the timestamp for the timer.
          Instant timestamp = Instant.now();
          if (residual.hasRequestedTimeDelay()) {
            timestamp =
                timestamp.plus(
                    Duration.millis(residual.getRequestedTimeDelay().getSeconds() * 1000));
          }
          // Calculate the watermark hold for the timer.
          long outputTimestamp = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
          if (!residual.getApplication().getOutputWatermarksMap().isEmpty()) {
            for (org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp outputWatermark :
                residual.getApplication().getOutputWatermarksMap().values()) {
              outputTimestamp = Math.min(outputTimestamp, outputWatermark.getSeconds() * 1000);
            }
          } else {
            outputTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis();
          }
          for (BoundedWindow window : stateValue.getWindows()) {
            StateNamespace stateNamespace = StateNamespaces.window(windowCoder, window);
            timerInternals.setTimer(
                stateNamespace,
                tag,
                "",
                timestamp,
                Instant.ofEpochMilli(outputTimestamp),
                TimeDomain.PROCESSING_TIME);
            stateInternals
                .state(stateNamespace, StateTags.value(tag, residualCoder))
                .write(
                    WindowedValue.of(
                        stateValue.getValue(),
                        stateValue.getTimestamp(),
                        ImmutableList.of(window),
                        stateValue.getPane()));
          }
        } catch (Exception e) {
          throw new RuntimeException("Failed to set timer/state for the residual", e);
        }
      }
    }
  }
}
