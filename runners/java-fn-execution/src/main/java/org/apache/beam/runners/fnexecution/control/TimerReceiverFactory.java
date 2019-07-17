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
package org.apache.beam.runners.fnexecution.control;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.TimerSpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link OutputReceiverFactory} that passes outputs to {@link
 * TimerReceiverFactory#timerDataConsumer}.
 */
public class TimerReceiverFactory implements OutputReceiverFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TimerReceiverFactory.class);

  /** Timer PCollection id => TimerReference. */
  private final HashMap<String, TimerSpec> timerOutputIdToSpecMap;

  private final BiConsumer<WindowedValue, TimerData> timerDataConsumer;
  private final Coder windowCoder;

  public TimerReceiverFactory(
      StageBundleFactory stageBundleFactory,
      BiConsumer<WindowedValue, TimerInternals.TimerData> timerDataConsumer,
      Coder windowCoder) {
    this.timerOutputIdToSpecMap = new HashMap<>();
    // Gather all timers from all transforms by their output pCollectionId which is unique
    for (Map<String, ProcessBundleDescriptors.TimerSpec> transformTimerMap :
        stageBundleFactory.getProcessBundleDescriptor().getTimerSpecs().values()) {
      for (ProcessBundleDescriptors.TimerSpec timerSpec : transformTimerMap.values()) {
        timerOutputIdToSpecMap.put(timerSpec.outputCollectionId(), timerSpec);
      }
    }
    this.timerDataConsumer = timerDataConsumer;
    this.windowCoder = windowCoder;
  }

  @Override
  public <OutputT> FnDataReceiver<OutputT> create(String pCollectionId) {
    final ProcessBundleDescriptors.TimerSpec timerSpec = timerOutputIdToSpecMap.get(pCollectionId);

    return receivedElement -> {
      WindowedValue windowedValue = (WindowedValue) receivedElement;
      Timer timer =
          Preconditions.checkNotNull(
              (Timer) ((KV) windowedValue.getValue()).getValue(),
              "Received null Timer from SDK harness: %s",
              receivedElement);
      LOG.debug("Timer received: {} {}", pCollectionId, timer);
      for (Object window : windowedValue.getWindows()) {
        StateNamespace namespace = StateNamespaces.window(windowCoder, (BoundedWindow) window);
        TimeDomain timeDomain = timerSpec.getTimerSpec().getTimeDomain();
        String timerId = timerSpec.inputCollectionId();
        TimerInternals.TimerData timerData =
            TimerInternals.TimerData.of(timerId, namespace, timer.getTimestamp(), timeDomain);
        timerDataConsumer.accept(windowedValue, timerData);
      }
    };
  }
}
