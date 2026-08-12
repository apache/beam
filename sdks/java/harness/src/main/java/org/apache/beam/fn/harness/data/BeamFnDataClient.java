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
package org.apache.beam.fn.harness.data;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;

/**
 * The {@link BeamFnDataClient} is able to forward inbound elements to a {@link FnDataReceiver} and
 * provide a receiver of outbound elements. Callers can register themselves as receivers for inbound
 * elements or can get a handle for a receiver of outbound elements.
 */
public interface BeamFnDataClient extends Closeable {
  /**
   * Registers a receiver for the provided instruction id.
   *
   * <p>The receiver is not required to be thread safe.
   *
   * <p>Receivers for successfully processed bundles must be unregistered. See {@link
   * #unregisterReceiver} for details.
   *
   * <p>Any failure during {@link FnDataReceiver#accept} will mark the provided {@code
   * instructionId} as invalid and will ignore any future data. It is expected that if a bundle
   * fails during processing then the failure will become visible to the {@link BeamFnDataClient}
   * during a future {@link FnDataReceiver#accept} invocation.
   */
  void registerReceiver(
      String instructionId,
      String dataStreamId,
      List<ApiServiceDescriptor> apiServiceDescriptors,
      CloseableFnDataReceiver<Elements> receiver);

  /**
   * Receivers are only expected to be unregistered when bundle processing has completed
   * successfully.
   *
   * <p>It is expected that if a bundle fails during processing then the failure will become visible
   * to the {@link BeamFnDataClient} during a future {@link FnDataReceiver#accept} invocation or via
   * a call to {@link #poisonInstructionId}.
   */
  void unregisterReceiver(
      String instructionId, String dataStreamId, List<ApiServiceDescriptor> apiServiceDescriptors);

  /**
   * Poisons the instruction id, indicating that future data arriving for it should be discarded.
   * Unregisters the receiver if was registered.
   *
   * @param instructionId
   */
  void poisonInstructionId(String instructionId);

  /** Get the outbound observer for the specified apiServiceDescriptor and dataStreamId. */
  StreamObserver<Elements> getOutboundObserver(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor, String dataStreamId);

  @Override
  default void close() throws IOException {
    // Default to no-op
  }
}
