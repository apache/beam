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
package org.apache.beam.runners.core;

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

/** Processes a bundle by sending it to an SDK harness over the Fn API. */
public class SdkHarnessDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {

  private final SdkHarnessClient sdkHarnessClient;
  private final String processBundleDescriptorId;

  /** {@code null} between bundles. */
  @Nullable private SdkHarnessClient.ActiveBundle activeBundle;

  private SdkHarnessDoFnRunner(
      SdkHarnessClient sdkHarnessClient,
      String processBundleDescriptorId) {
    this.sdkHarnessClient = sdkHarnessClient;
    this.processBundleDescriptorId = processBundleDescriptorId;
  }

  /**
   * Returns a new {@link SdkHarnessDoFnRunner} suitable for just a particular {@link
   * org.apache.beam.fn.v1.BeamFnApi.ProcessBundleDescriptor} (referenced by id here).
   *
   * <p>The {@link FnDataReceiver} must be the correct data plane service referenced
   * in the primitive instructions in the
   * {@link org.apache.beam.fn.v1.BeamFnApi.ProcessBundleDescriptor}.
   *
   * <p>Also outside of this class, the appropriate receivers must be registered with the
   * output data plane channels of the descriptor.
   */
  public static <InputT, OutputT> SdkHarnessDoFnRunner<InputT, OutputT> create(
      SdkHarnessClient sdkHarnessClient,
      String processBundleDescriptorId) {
    return new SdkHarnessDoFnRunner(sdkHarnessClient, processBundleDescriptorId);
  }

  @Override
  public void startBundle() {
    this.activeBundle =
        sdkHarnessClient.newBundle(processBundleDescriptorId);
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    checkState(
        activeBundle != null,
        "%s attempted to process an element without an active bundle",
        SdkHarnessDoFnRunner.class.getSimpleName());

    try {
      activeBundle.getInputReceiver().accept(elem);
    } catch (Exception exc) {
      throw new RuntimeException(exc);
    }
  }

  @Override
  public void onTimer(
      String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
    throw new UnsupportedOperationException("Timers are not supported over the Fn API");
  }

  @Override
  public void finishBundle() {
    try {
      activeBundle.getBundleResponse().get();
    } catch (InterruptedException interrupted) {
      Thread.interrupted();
      return;
    } catch (ExecutionException exc) {
      throw UserCodeException.wrap(exc);
    }
  }
}
