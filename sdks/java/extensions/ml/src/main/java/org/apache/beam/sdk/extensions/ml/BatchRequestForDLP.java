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

/**
 * DoFn batching the input PCollection into bigger requests in order to better utilize the Cloud DLP
 * service.
 */
@Experimental
class BatchRequestForDLP extends DoFn<KV<String, String>, KV<String, String>> {
  public static final Logger LOG = LoggerFactory.getLogger(BatchRequestForDLP.class);
  private final Counter numberOfElementsBagged =
      Metrics.counter(BatchRequestForDLP.class, "numberOfElementsBagged");
  private final Integer batchSize;

  public BatchRequestForDLP(Integer batchSize) {
    this.batchSize = batchSize;
  }

  @StateId("elementsBag")
  private final StateSpec<BagState<KV<String, String>>> elementsBag = StateSpecs.bag();

  @TimerId("eventTimer")
  private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @ProcessElement
  public void process(
      @Element KV<String, String> element,
      @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
      @TimerId("eventTimer") Timer eventTimer,
      BoundedWindow w) {
    elementsBag.add(element);
    eventTimer.set(w.maxTimestamp());
  }

  @OnTimer("eventTimer")
  public void onTimer(
      @StateId("elementsBag") BagState<KV<String, String>> elementsBag,
      OutputReceiver<KV<String, Iterable<String>>> output) {
    String key = elementsBag.read().iterator().next().getKey();
    AtomicInteger bufferSize = new AtomicInteger();
    List<String> rows = new ArrayList<>();
    elementsBag
        .read()
        .forEach(
            element -> {
              int elementSize = element.getValue().getBytes(UTF_8).length;
              boolean clearBuffer = bufferSize.intValue() + elementSize > batchSize;
              if (clearBuffer) {
                numberOfElementsBagged.inc(rows.size());
                LOG.debug("Clear Buffer {} , Key {}", bufferSize.intValue(), element.getKey());
                output.output(KV.of(element.getKey(), rows));
                rows.clear();
                bufferSize.set(0);
              }
              rows.add(element.getValue());
              bufferSize.getAndAdd(element.getValue().getBytes(UTF_8).length);
            });
    if (!rows.isEmpty()) {
      LOG.debug("Remaining rows {}", rows.size());
      numberOfElementsBagged.inc(rows.size());
      output.output(KV.of(key, rows));
    }
  }
}
