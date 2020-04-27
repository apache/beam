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
package org.apache.beam.sdk.extensions.ml;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

/**
 * DoFn batching the input PCollection into bigger requests in order to better utilize the Cloud DLP
 * service.
 */
class BatchRequestForDLP extends DoFn<KV<String, String>, KV<String, String>> {
  private final Integer batchSize;
  public static final Integer DLP_PAYLOAD_LIMIT = 52400;

  public BatchRequestForDLP(Integer batchSize) {
    if (batchSize > DLP_PAYLOAD_LIMIT) {
      throw new IllegalArgumentException(
          "DLP batch size exceeds payload limit.\n"
              + "Batch size should be smaller than "
              + DLP_PAYLOAD_LIMIT);
    }
    this.batchSize = batchSize;
  }

  @StateId("elementsBag")
  private final StateSpec<BagState<KV<String, String>>> elementsBag = StateSpecs.bag();

  @StateId("elementsSize")
  private final StateSpec<ValueState<Integer>> elementsSize = StateSpecs.value();

  @TimerId("eventTimer")
  private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @ProcessElement
  public void process(
      @Element KV<String, String> element,
      @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
      @StateId("elementsSize") ValueState<Integer> elementsSize,
      @Timestamp Instant elementTs,
      @TimerId("eventTimer") Timer eventTimer,
      OutputReceiver<KV<String, String>> output) {
    eventTimer.set(elementTs);
    Integer currentElementSize =
        (element.getValue() == null) ? 0 : element.getValue().getBytes(UTF_8).length;
    Integer currentBufferSize = (elementsSize.read() == null) ? 0 : elementsSize.read();
    boolean clearBuffer = (currentElementSize + currentBufferSize) > batchSize;
    if (clearBuffer) {
      KV<String, String> inspectBufferedData = emitResult(elementsBag.read());
      output.output(inspectBufferedData);
      DLPInspectText.LOG.info(
          "****CLEAR BUFFER Key {} **** Current Content Size {}",
          inspectBufferedData.getKey(),
          inspectBufferedData.getValue().getBytes(UTF_8).length);
      clearState(elementsBag, elementsSize);
    } else {
      elementsBag.add(element);
      elementsSize.write(currentElementSize + currentBufferSize);
    }
  }

  @OnTimer("eventTimer")
  public void onTimer(
      @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
      @StateId("elementsSize") ValueState<Integer> elementsSize,
      OutputReceiver<KV<String, String>> output) {
    // Process left over records less than  batch size
    KV<String, String> inspectBufferedData = emitResult(elementsBag.read());
    output.output(inspectBufferedData);
    DLPInspectText.LOG.info(
        "****Timer Triggered Key {} **** Current Content Size {}",
        inspectBufferedData.getKey(),
        inspectBufferedData.getValue().getBytes(UTF_8).length);
    clearState(elementsBag, elementsSize);
  }

  private static KV<String, String> emitResult(Iterable<KV<String, String>> bufferData) {
    StringBuilder builder = new StringBuilder();
    String fileName =
        (bufferData.iterator().hasNext()) ? bufferData.iterator().next().getKey() : "UNKNOWN_FILE";
    bufferData.forEach(
        e -> {
          builder.append(e.getValue());
        });
    return KV.of(fileName, builder.toString());
  }

  private static void clearState(
      BagState<KV<String, String>> elementsBag, ValueState<Integer> elementsSize) {
    elementsBag.clear();
    elementsSize.clear();
  }
}
