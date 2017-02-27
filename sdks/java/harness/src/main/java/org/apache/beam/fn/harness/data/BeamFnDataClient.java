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

import java.util.concurrent.CompletableFuture;
import org.apache.beam.fn.harness.fn.CloseableThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

/**
 * The {@link BeamFnDataClient} is able to forward inbound elements to a consumer and is also a
 * consumer of outbound elements. Callers can register themselves as consumers for inbound elements
 * or can get a handle for a consumer for outbound elements.
 */
public interface BeamFnDataClient {
  /**
   * Registers the following inbound consumer for the provided instruction id and target.
   *
   * <p>The provided coder is used to decode inbound elements. The decoded elements
   * are passed to the provided consumer. Any failure during decoding or processing of the element
   * will complete the returned future exceptionally. On successful termination of the stream,
   * the returned future is completed successfully.
   *
   * <p>The consumer is not required to be thread safe.
   */
  <T> CompletableFuture<Void> forInboundConsumer(
      BeamFnApi.ApiServiceDescriptor apiServiceDescriptor,
      KV<String, BeamFnApi.Target> inputLocation,
      Coder<WindowedValue<T>> coder,
      ThrowingConsumer<WindowedValue<T>> consumer);

  /**
   * Creates a closeable consumer using the provided instruction id and target.
   *
   * <p>The provided coder is used to encode elements on the outbound stream.
   *
   * <p>Closing the returned consumer signals the end of the stream.
   *
   * <p>The returned closeable consumer is not thread safe.
   */
  <T> CloseableThrowingConsumer<WindowedValue<T>> forOutboundConsumer(
      BeamFnApi.ApiServiceDescriptor apiServiceDescriptor,
      KV<String, BeamFnApi.Target> outputLocation,
      Coder<WindowedValue<T>> coder);
}
