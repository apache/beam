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

import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;

/**
 * The {@link BeamFnTimerClient} is able to forward inbound timers to a {@link FnDataReceiver} and
 * provide a receiver for outbound timers. Callers can register a timer {@link LogicalEndpoint} to
 * both send and receive timers.
 */
public interface BeamFnTimerClient {
  /**
   * A handler capable of blocking for inbound timers and also capable of consuming outgoing timers.
   * See {@link InboundDataClient} for all details related to handling inbound timers and see {@link
   * CloseableFnDataReceiver} for all details related to sending outbound timers.
   */
  interface TimerHandler<K> extends InboundDataClient, CloseableFnDataReceiver<Timer<K>> {}

  /**
   * Registers for a timer handler for for the provided instruction id and target.
   *
   * <p>The provided coder is used to encode and decode timers. The inbound timers are passed to the
   * provided receiver. Any failure during decoding or processing of the timer will complete the
   * timer handler exceptionally. On successful termination of the stream, the returned timer
   * handler is completed successfully.
   *
   * <p>The receiver is not required to be thread safe.
   */
  <K> TimerHandler<K> register(
      LogicalEndpoint timerEndpoint, Coder<Timer<K>> coder, FnDataReceiver<Timer<K>> receiver);
}
