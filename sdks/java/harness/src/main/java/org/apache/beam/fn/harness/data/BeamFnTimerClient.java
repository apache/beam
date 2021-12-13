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

import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;

/**
 * The {@link BeamFnTimerClient} is able to register to forward outbound timers to a {@link
 * FnDataReceiver}. Callers can register to receive timers via {@link
 * org.apache.beam.fn.harness.PTransformRunnerFactory.Context#addIncomingTimerEndpoint}.
 */
public interface BeamFnTimerClient {
  /**
   * Registers for a timer handler for the provided logical endpoint.
   *
   * <p>The provided coder is used to encode timers. The outbound timers should be passed to the
   * returned receiver.
   */
  <K> CloseableFnDataReceiver<Timer<K>> register(
      LogicalEndpoint logicalEndpoint,
      Coder<Timer<K>> coder,
      Consumer<Elements> responseEmbedElementsConsumer);
}
