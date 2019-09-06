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
package org.apache.beam.runners.fnexecution.data;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;

/**
 * The {@link FnDataService} is able to forward inbound elements to a consumer and is also a
 * consumer of outbound elements. Callers can register themselves as consumers for inbound elements
 * or can get a handle for a consumer for outbound elements.
 */
public interface FnDataService {

  /**
   * Registers a receiver to be notified upon any incoming elements.
   *
   * <p>The provided coder is used to decode inbound elements. The decoded elements are passed to
   * the provided receiver.
   *
   * <p>Any failure during decoding or processing of the element will put the {@link
   * InboundDataClient} into an error state such that {@link InboundDataClient#awaitCompletion()}
   * will throw an exception.
   *
   * <p>The provided receiver is not required to be thread safe.
   */
  <T> InboundDataClient receive(
      LogicalEndpoint inputLocation, Coder<T> coder, FnDataReceiver<T> listener);

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
  <T> CloseableFnDataReceiver<T> send(LogicalEndpoint outputLocation, Coder<T> coder);
}
