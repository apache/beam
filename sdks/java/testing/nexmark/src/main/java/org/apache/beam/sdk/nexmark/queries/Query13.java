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
package org.apache.beam.sdk.nexmark.queries;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle.AssignShardFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Query "13" PORTABILITY_BATCH (not in original suite).
 *
 * <p>This benchmark is created to stress the boundary of runner and SDK in portability world. The
 * basic shape of this benchmark is source + GBK + ParDo, in which the GBK read + ParDo will require
 * that runner reads from shuffle and connects with SDK to do CPU intensive computation.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class Query13 extends NexmarkQueryTransform<Event> {
  private final NexmarkConfiguration configuration;

  public Query13(NexmarkConfiguration configuration) {
    super("Query13");
    this.configuration = configuration;
  }

  @Override
  public PCollection<Event> expand(PCollection<Event> events) {
    final Coder<Event> coder = events.getCoder();
    return events
        .apply("Pair with random key", ParDo.of(new AssignShardFn<>(configuration.numKeyBuckets)))
        .apply(GroupByKey.create())
        .apply(
            "ExpandIterable",
            ParDo.of(
                new DoFn<KV<Integer, Iterable<Event>>, Event>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<Integer, Iterable<Event>> element, OutputReceiver<Event> r) {
                    for (Event value : element.getValue()) {
                      r.output(value);
                    }
                  }
                }))
        // Force round trip through coder.
        .apply(
            name + ".Serialize",
            ParDo.of(
                new DoFn<Event, Event>() {
                  private final Counter bytesMetric = Metrics.counter(name, "serde-bytes");
                  private final Random random = new Random();
                  private double pardoCPUFactor =
                      (configuration.pardoCPUFactor >= 0.0 && configuration.pardoCPUFactor <= 1.0)
                          ? configuration.pardoCPUFactor
                          : 1.0;

                  @ProcessElement
                  public void processElement(ProcessContext c) throws CoderException, IOException {
                    Event event;
                    if (random.nextDouble() <= pardoCPUFactor) {
                      event = encodeDecode(coder, c.element(), bytesMetric);
                    } else {
                      event = c.element();
                    }
                    c.output(event);
                  }
                }));
  }

  private static Event encodeDecode(Coder<Event> coder, Event e, Counter bytesMetric)
      throws IOException {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    coder.encode(e, outStream, Coder.Context.OUTER);
    byte[] byteArray = outStream.toByteArray();
    bytesMetric.inc((long) byteArray.length);
    ByteArrayInputStream inStream = new ByteArrayInputStream(byteArray);
    return coder.decode(inStream, Coder.Context.OUTER);
  }
}
