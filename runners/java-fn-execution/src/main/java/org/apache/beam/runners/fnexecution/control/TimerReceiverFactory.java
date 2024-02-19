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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.TimerSpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory that passes timers to {@link TimerReceiverFactory#timerDataConsumer}.
 *
 * <p>The constructed timers uses {@code len(transformId):transformId:timerId} as the timer id to
 * prevent string collisions. See {@link #encodeToTimerDataTimerId} and {@link
 * #decodeTimerDataTimerId} for functions to aid with encoding and decoding.
 *
 * <p>If the incoming timer is being cleared, the {@link TimerData} sets the fire and hold
 * timestamps to {@link BoundedWindow#TIMESTAMP_MAX_VALUE}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TimerReceiverFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TimerReceiverFactory.class);

  /** (PTransform, Timer Local Name) => TimerReference. */
  private final HashMap<KV<String, String>, TimerSpec> transformAndTimerIdToSpecMap;

  private final BiConsumer<Timer<?>, TimerData> timerDataConsumer;
  private final Coder windowCoder;

  public TimerReceiverFactory(
      StageBundleFactory stageBundleFactory,
      BiConsumer<Timer<?>, TimerInternals.TimerData> timerDataConsumer,
      Coder windowCoder) {
    this.transformAndTimerIdToSpecMap = new HashMap<>();
    // Create a lookup map using the transform and timerId as the key.
    for (Map<String, ProcessBundleDescriptors.TimerSpec> transformTimerMap :
        stageBundleFactory.getProcessBundleDescriptor().getTimerSpecs().values()) {
      for (ProcessBundleDescriptors.TimerSpec timerSpec : transformTimerMap.values()) {
        transformAndTimerIdToSpecMap.put(
            KV.of(timerSpec.transformId(), timerSpec.timerId()), timerSpec);
      }
    }
    this.timerDataConsumer = timerDataConsumer;
    this.windowCoder = windowCoder;
  }

  // @Override
  public <K> FnDataReceiver<Timer<K>> create(String transformId, String timerFamilyId) {
    final ProcessBundleDescriptors.TimerSpec timerSpec =
        transformAndTimerIdToSpecMap.get(KV.of(transformId, timerFamilyId));

    return receivedElement -> {
      Timer timer =
          checkNotNull(
              receivedElement, "Received null Timer from SDK harness: %s", receivedElement);
      LOG.debug("Timer received: {}", timer);
      for (Object window : timer.getWindows()) {
        StateNamespace namespace = StateNamespaces.window(windowCoder, (BoundedWindow) window);
        TimerInternals.TimerData timerData =
            TimerInternals.TimerData.of(
                timer.getDynamicTimerTag(),
                encodeToTimerDataTimerId(timerSpec.transformId(), timerSpec.timerId()),
                namespace,
                timer.getClearBit() ? BoundedWindow.TIMESTAMP_MAX_VALUE : timer.getFireTimestamp(),
                timer.getClearBit() ? BoundedWindow.TIMESTAMP_MAX_VALUE : timer.getHoldTimestamp(),
                timerSpec.getTimerSpec().getTimeDomain());
        timerDataConsumer.accept(timer, timerData);
      }
    };
  }

  /**
   * Encodes transform and timer family ids into a single string which retains the human readable
   * format {@code len(transformId):transformId:timerId}. See {@link #decodeTimerDataTimerId} for
   * decoding.
   */
  public static String encodeToTimerDataTimerId(String transformId, String timerFamilyId) {
    return transformId.length() + ":" + transformId + ":" + timerFamilyId;
  }

  /**
   * Decodes a string into the transform and timer family ids. See {@link #encodeToTimerDataTimerId}
   * for encoding.
   */
  public static KV<String, String> decodeTimerDataTimerId(String timerDataTimerId) {
    int transformIdLengthSplit = timerDataTimerId.indexOf(":");
    if (transformIdLengthSplit <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid encoding, expected len(transformId):transformId:timerId as the encoding but received %s",
              timerDataTimerId));
    }
    int transformIdLength = Integer.parseInt(timerDataTimerId.substring(0, transformIdLengthSplit));
    String transformId =
        timerDataTimerId.substring(
            transformIdLengthSplit + 1, transformIdLengthSplit + 1 + transformIdLength);
    String timerFamilyId =
        timerDataTimerId.substring(transformIdLengthSplit + 1 + transformIdLength + 1);
    return KV.of(transformId, timerFamilyId);
  }
}
