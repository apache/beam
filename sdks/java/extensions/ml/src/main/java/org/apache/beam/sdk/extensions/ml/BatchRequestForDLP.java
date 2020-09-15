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

import com.google.privacy.dlp.v2.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Batches input rows to reduce number of requests sent to Cloud DLP service. */
@Experimental
class BatchRequestForDLP extends DoFn<KV<String, Table.Row>, KV<String, Iterable<Table.Row>>> {
  public static final Logger LOG = LoggerFactory.getLogger(BatchRequestForDLP.class);

  private final Counter numberOfRowsBagged =
      Metrics.counter(BatchRequestForDLP.class, "numberOfRowsBagged");

  private final Integer batchSizeBytes;

  @StateId("elementsBag")
  private final StateSpec<BagState<KV<String, Table.Row>>> elementsBag = StateSpecs.bag();

  @TimerId("eventTimer")
  private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  /**
   * Constructs the batching DoFn.
   *
   * @param batchSize Desired batch size in bytes.
   */
  public BatchRequestForDLP(Integer batchSize) {
    this.batchSizeBytes = batchSize;
  }

  @ProcessElement
  public void process(
      @Element KV<String, Table.Row> element,
      @StateId("elementsBag") BagState<KV<String, Table.Row>> elementsBag,
      @TimerId("eventTimer") Timer eventTimer,
      BoundedWindow w) {
    elementsBag.add(element);
    eventTimer.set(w.maxTimestamp());
  }

  /**
   * Outputs the elements buffered in the elementsBag in batches of desired size.
   *
   * @param elementsBag element buffer.
   * @param output Batched input elements.
   */
  @OnTimer("eventTimer")
  public void onTimer(
      @StateId("elementsBag") BagState<KV<String, Table.Row>> elementsBag,
      OutputReceiver<KV<String, Iterable<Table.Row>>> output) {
    if (elementsBag.read().iterator().hasNext()) {
      String key = elementsBag.read().iterator().next().getKey();
      AtomicInteger bufferSize = new AtomicInteger();
      List<Table.Row> rows = new ArrayList<>();
      elementsBag
          .read()
          .forEach(
              element -> {
                int elementSize = element.getValue().getSerializedSize();
                boolean clearBuffer = bufferSize.intValue() + elementSize > batchSizeBytes;
                if (clearBuffer) {
                  LOG.debug(
                      "Clear buffer of {} bytes, Key {}", bufferSize.intValue(), element.getKey());
                  numberOfRowsBagged.inc(rows.size());
                  output.output(KV.of(element.getKey(), rows));
                  rows.clear();
                  bufferSize.set(0);
                }
                rows.add(element.getValue());
                bufferSize.getAndAdd(element.getValue().getSerializedSize());
              });
      if (!rows.isEmpty()) {
        LOG.debug("Outputting remaining {} rows.", rows.size());
        numberOfRowsBagged.inc(rows.size());
        output.output(KV.of(key, rows));
      }
    }
  }
}
