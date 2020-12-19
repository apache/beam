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

import com.google.api.client.json.GenericJson;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
 * DoFn that batches up GenericJson to reduce the number of requests made to the Recommendations
 * API.
 */
class BatchRequestForRecommendationAI
    extends DoFn<KV<String, GenericJson>, KV<String, Iterable<GenericJson>>> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchRequestForRecommendationAI.class);

  private final Counter numberOfRowsBagged =
      Metrics.counter(BatchRequestForRecommendationAI.class, "numberOfRowsBagged");

  private final Integer maxBatchSize;

  @StateId("elementsBag")
  private final StateSpec<BagState<KV<String, GenericJson>>> elementsBag = StateSpecs.bag();

  @TimerId("eventTimer")
  private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  /**
   * Constructs the batching DoFn.
   *
   * @param maxBatchSize Desired batch size in bytes.
   */
  public BatchRequestForRecommendationAI(Integer maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
  }

  @ProcessElement
  public void process(
      @Element KV<String, GenericJson> element,
      @StateId("elementsBag") BagState<KV<String, GenericJson>> elementsBag,
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
      @StateId("elementsBag") BagState<KV<String, GenericJson>> elementsBag,
      OutputReceiver<KV<String, Iterable<GenericJson>>> output) {
    if (elementsBag.read().iterator().hasNext()) {
      String key = elementsBag.read().iterator().next().getKey();
      AtomicInteger currentBatchSize = new AtomicInteger();
      List<GenericJson> rows = new ArrayList<>();
      elementsBag
          .read()
          .forEach(
              element -> {
                boolean clearBuffer = currentBatchSize.intValue() > maxBatchSize;
                if (clearBuffer) {
                  LOG.debug(
                      "Clear buffer of {} items, Key {}",
                      currentBatchSize.intValue(),
                      element.getKey());
                  numberOfRowsBagged.inc(rows.size());
                  output.output(KV.of(element.getKey(), rows));
                  rows.clear();
                  currentBatchSize.set(0);
                }
                rows.add(element.getValue());
                currentBatchSize.getAndAdd(1);
              });
      if (!rows.isEmpty()) {
        LOG.debug("Outputting remaining {} rows.", rows.size());
        numberOfRowsBagged.inc(rows.size());
        output.output(KV.of(key, rows));
      }
    }
  }
}
