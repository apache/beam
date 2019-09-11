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

import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;

/**
 * The {@link BeamFnDataClient} is able to forward inbound elements to a {@link FnDataReceiver} and
 * provide a receiver of outbound elements. Callers can register themselves as receivers for inbound
 * elements or can get a handle for a receiver of outbound elements.
 */
public interface BeamFnDataClient {
  /**
   * Registers the following inbound receiver for the provided instruction id and target.
   *
   * <p>The provided coder is used to decode inbound elements. The decoded elements are passed to
   * the provided receiver. Any failure during decoding or processing of the element will complete
   * the returned future exceptionally. On successful termination of the stream, the returned future
   * is completed successfully.
   *
   * <p>The receiver is not required to be thread safe.
   */
  <T> InboundDataClient receive(
      ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint inputLocation,
      Coder<T> coder,
      FnDataReceiver<T> receiver);

  /**
   * Creates a {@link CloseableFnDataReceiver} using the provided instruction id and target.
   *
   * <p>The provided coder is used to encode elements on the outbound stream.
   *
   * <p>Closing the returned receiver signals the end of the stream.
   *
   * <p>The returned closeable receiver is not thread safe.
   */
  <T> CloseableFnDataReceiver<T> send(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint outputLocation,
      Coder<T> coder);
}
