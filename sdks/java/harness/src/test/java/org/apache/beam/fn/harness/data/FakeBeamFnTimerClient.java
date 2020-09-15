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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;

/** An implementation of a {@link BeamFnTimerClient} that can be used for testing. */
public class FakeBeamFnTimerClient implements BeamFnTimerClient {
  private final ConcurrentMap<LogicalEndpoint, TimerHandler<?>> timerHandlers;
  private final ConcurrentMap<LogicalEndpoint, List<Timer<?>>> setTimers;
  private final ConcurrentMap<LogicalEndpoint, Boolean> wasClosed;
  private final ConcurrentMap<LogicalEndpoint, CompletableFuture> timerInputFutures;
  private final ConcurrentMap<LogicalEndpoint, FnDataReceiver<Timer<?>>> timerInputReceivers;

  public FakeBeamFnTimerClient() {
    this.timerHandlers = new ConcurrentHashMap<>();
    this.setTimers = new ConcurrentHashMap<>();
    this.wasClosed = new ConcurrentHashMap<>();
    this.timerInputFutures = new ConcurrentHashMap<>();
    this.timerInputReceivers = new ConcurrentHashMap<>();
  }

  @Override
  public <K> TimerHandler<K> register(
      LogicalEndpoint timerEndpoint, Coder<Timer<K>> coder, FnDataReceiver<Timer<K>> receiver) {
    return (TimerHandler)
        timerHandlers.computeIfAbsent(
            timerEndpoint,
            (endpoint) -> {
              setTimers.put(timerEndpoint, new ArrayList<>());
              wasClosed.put(timerEndpoint, false);
              timerInputFutures.put(timerEndpoint, new CompletableFuture());
              timerInputReceivers.put(timerEndpoint, (FnDataReceiver) receiver);

              return new TimerHandler<Object>() {
                @Override
                public void awaitCompletion() throws InterruptedException, Exception {
                  timerInputFutures.get(endpoint).get();
                }

                @Override
                public boolean isDone() {
                  return timerInputFutures.get(endpoint).isDone();
                }

                @Override
                public void cancel() {
                  timerInputFutures.get(endpoint).cancel(true);
                }

                @Override
                public void complete() {
                  timerInputFutures.get(endpoint).complete(null);
                }

                @Override
                public void fail(Throwable t) {
                  timerInputFutures.get(endpoint).completeExceptionally(t);
                }

                @Override
                public void accept(Timer<Object> input) throws Exception {
                  setTimers.get(endpoint).add(input);
                }

                @Override
                public void flush() throws Exception {}

                @Override
                public void close() throws Exception {
                  wasClosed.put(endpoint, true);
                }
              };
            });
  }

  public void sendTimer(LogicalEndpoint timerEndpoint, Timer<?> timer) throws Exception {
    timerInputReceivers.get(timerEndpoint).accept(timer);
  }

  public void closeInbound(LogicalEndpoint timerEndpoint) {
    timerInputFutures.get(timerEndpoint).complete(null);
  }

  public boolean isOutboundClosed(LogicalEndpoint timerEndpoint) {
    return wasClosed.get(timerEndpoint);
  }

  public List<Timer<?>> getTimers(LogicalEndpoint timerEndpoint) {
    return setTimers.get(timerEndpoint);
  }
}
