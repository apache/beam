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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.runners.fnexecution.control.BundleCheckpointHandlers.StateAndTimerBundleCheckpointHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BundleCheckpointHandlers}. */
@RunWith(JUnit4.class)
public class BundleCheckpointHandlersTest {

  private static final Coder<WindowedValue<String>> RESIDUAL_CODER =
      WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);

  /**
   * Regression test for https://github.com/apache/beam/issues/27648.
   *
   * <p>A polling SDF self-checkpoints on every poll via {@code tracker.defer_remainder()}. Each
   * self-checkpoint used to store its residual under a brand new, unique state tag
   * ("sdf_checkpoint:&lt;id&gt;:&lt;index&gt;"), so every poll registered another keyed-state
   * descriptor and the job leaked heap without bound. All residuals for a key/window must instead
   * live under a single, stable state descriptor.
   */
  @Test
  public void repeatedSelfCheckpointsUseBoundedStateDescriptors() throws Exception {
    Set<String> stateDescriptorIds = new HashSet<>();
    StateInternalsFactory<String> stateInternalsFactory =
        key -> new RecordingStateInternals(InMemoryStateInternals.forKey(key), stateDescriptorIds);
    TimerInternalsFactory<String> timerInternalsFactory = key -> new InMemoryTimerInternals();

    StateAndTimerBundleCheckpointHandler<String> handler =
        new StateAndTimerBundleCheckpointHandler<>(
            timerInternalsFactory,
            stateInternalsFactory,
            RESIDUAL_CODER,
            GlobalWindow.Coder.INSTANCE);

    int selfCheckpoints = 100;
    for (int i = 0; i < selfCheckpoints; i++) {
      handler.onCheckpoint(residualResponse("key"));
    }

    // One stable descriptor for all residuals, no matter how many times the SDF checkpointed.
    assertThat(stateDescriptorIds, hasSize(1));
  }

  private static ProcessBundleResponse residualResponse(String key) throws Exception {
    // The residual element's value is used as the SDF key.
    byte[] encodedElement =
        CoderUtils.encodeToByteArray(RESIDUAL_CODER, WindowedValues.valueInGlobalWindow(key));
    return ProcessBundleResponse.newBuilder()
        .addResidualRoots(
            DelayedBundleApplication.newBuilder()
                .setApplication(
                    BundleApplication.newBuilder()
                        .setElement(ByteString.copyFrom(encodedElement))
                        .build())
                .build())
        .build();
  }

  /** A {@link StateInternals} that records every state descriptor id it is asked for. */
  private static class RecordingStateInternals implements StateInternals {
    private final StateInternals delegate;
    private final Set<String> descriptorIds;

    RecordingStateInternals(StateInternals delegate, Set<String> descriptorIds) {
      this.delegate = delegate;
      this.descriptorIds = descriptorIds;
    }

    @Override
    public Object getKey() {
      return delegate.getKey();
    }

    @Override
    public <T extends State> T state(
        StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
      descriptorIds.add(address.getId());
      return delegate.state(namespace, address, c);
    }
  }
}
