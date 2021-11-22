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

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements.Timers;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;

/**
 * ElementsTimersDispatcher dispatches in memory stored timers to the corresponding timer consumers.
 * dispatch() method should be invoked at the process timer stage of the DoFn transform.
 */
public class ElementsTimersDispatcher {
  private final Elements elements;
  private final Map<String, Coder<Timer<Object>>> coders = new HashMap<>();
  private final Map<String, FnDataReceiver<Timer<Object>>> timerConsumers = new HashMap<>();

  public ElementsTimersDispatcher(Elements elements) {
    this.elements = elements;
  }

  // Register the consumer for the timerIdOrTimerFamilyId.
  public void registerTimerConsumer(
      String timerIdOrTimerFamilyId,
      Coder<Timer<Object>> coder,
      FnDataReceiver<Timer<Object>> consumer) {
    this.coders.put(timerIdOrTimerFamilyId, coder);
    this.timerConsumers.put(timerIdOrTimerFamilyId, consumer);
  }

  /**
   * Dispatches the timer objects to the corresponding consumers.
   *
   * @throws Exception if Coder failed to decode the timers bytes or any consumer fails to process
   *     the decoded timer object.
   */
  public void dispatch() throws Exception {
    if (this.elements == null || this.elements.getTimersList() == null) {
      return;
    }
    for (Timers timers : elements.getTimersList()) {
      if (timers.getIsLast()) {
        // Runner probably won't send timer that has last bit set if it is embedded in
        // ProcessBundleRequest, but to be safe we skip it if seen.
        continue;
      }
      String timerIdOrTimerFamilyId = timers.getTimerFamilyId();
      if (!coders.containsKey(timerIdOrTimerFamilyId)
          || !timerConsumers.containsKey(timerIdOrTimerFamilyId)) {
        throw new IllegalStateException(
            String.format("Consumer for timer id %s not registered.", timerIdOrTimerFamilyId));
      }
      Timer<Object> timer =
          coders.get(timerIdOrTimerFamilyId).decode(timers.getTimers().newInput());
      timerConsumers.get(timerIdOrTimerFamilyId).accept(timer);
    }
  }
}
