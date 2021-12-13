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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;

/** An implementation of a {@link BeamFnTimerClient} that can be used for testing. */
public class FakeBeamFnTimerClient implements BeamFnTimerClient {
  private final ConcurrentMap<LogicalEndpoint, List<Timer<?>>> setTimers;
  private final ConcurrentMap<LogicalEndpoint, Boolean> wasClosed;

  public FakeBeamFnTimerClient() {
    this.setTimers = new ConcurrentHashMap<>();
    this.wasClosed = new ConcurrentHashMap<>();
  }

  @Override
  public <K> CloseableFnDataReceiver<Timer<K>> register(
      LogicalEndpoint timerEndpoint,
      Coder<Timer<K>> coder,
      Consumer<Elements> embedOutputElementsConsumer) {
    setTimers.put(timerEndpoint, new ArrayList<>());
    wasClosed.put(timerEndpoint, false);

    return new CloseableFnDataReceiver<Timer<K>>() {
      @Override
      public void flush() throws Exception {}

      @Override
      public void close() throws Exception {
        wasClosed.put(timerEndpoint, true);
      }

      @Override
      public void accept(Timer<K> input) throws Exception {
        setTimers.get(timerEndpoint).add(input);
      }
    };
  }

  public boolean isOutboundClosed(LogicalEndpoint timerEndpoint) {
    return wasClosed.get(timerEndpoint);
  }

  public List<Timer<?>> getTimers(LogicalEndpoint timerEndpoint) {
    return setTimers.get(timerEndpoint);
  }
}
