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
package org.apache.beam.runners.core.fn;

import com.google.auto.value.AutoValue;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * The {@link FnDataService} is able to forward inbound elements to a consumer and is also a
 * consumer of outbound elements. Callers can register themselves as consumers for inbound elements
 * or can get a handle for a consumer for outbound elements.
 *
 * @deprecated Runners should depend on the beam-runners-java-fn-execution module for this
 *     functionality.
 */
@Deprecated
public interface FnDataService {

  /**
   * A logical endpoint is a pair of an instruction ID corresponding to the {@link
   * BeamFnApi.ProcessBundleRequest} and the {@link
   * BeamFnApi.Target} within the processing graph. This enables the same
   * {@link FnDataService} to be re-used across multiple bundles.
   */
  @AutoValue
  abstract class LogicalEndpoint {

    public abstract String getInstructionId();

    public abstract BeamFnApi.Target getTarget();

    public static LogicalEndpoint of(String instructionId, BeamFnApi.Target target) {
      return new AutoValue_FnDataService_LogicalEndpoint(instructionId, target);
    }
  }

  /**
   * Registers a receiver to be notified upon any incoming elements.
   *
   * <p>The provided coder is used to decode inbound elements. The decoded elements are passed to
   * the provided receiver.
   *
   * <p>Any failure during decoding or processing of the element will complete the returned future
   * exceptionally. On successful termination of the stream, the returned future is completed
   * successfully.
   *
   * <p>The provided receiver is not required to be thread safe.
   */
  <T> ListenableFuture<Void> listen(
      LogicalEndpoint inputLocation,
      Coder<WindowedValue<T>> coder,
      FnDataReceiver<WindowedValue<T>> listener)
      throws Exception;

  /**
   * Creates a receiver to which you can write data values and have them sent over this data plane
   * service.
   *
   * <p>The provided coder is used to encode elements on the outbound stream.
   *
   * <p>Closing the returned receiver signals the end of the stream.
   *
   * <p>The returned receiver is not thread safe.
   */
  <T> FnDataReceiver<WindowedValue<T>> send(
      LogicalEndpoint outputLocation, Coder<WindowedValue<T>> coder) throws Exception;
}
