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
package org.apache.beam.runners.kafka.streams.translation;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.kafka.streams.KafkaStreamsTestRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

/**
 * End-to-end test that GroupByKey groups per fixed window, not just per key: {@code Impulse -> emit
 * timestamped KVs -> Window.into(FixedWindows) -> GroupByKey -> record groups}.
 *
 * <p>The same key "a" has values in two different windows, so a correct windowed GroupByKey emits
 * two groups for it (one per window) rather than one combined group. This exercises the {@link
 * WindowedGroupByKeyProcessor} path (ReduceFnRunner over the Kafka Streams state and timer stores)
 * that the earlier global-window GroupByKey did not.
 */
public class FixedWindowGroupByKeyTest {

  private static final Duration WINDOW_SIZE = Duration.millis(10);

  /** Emits KVs whose timestamps fall into two adjacent fixed windows. */
  private static class EmitTimestampedKvsFn extends DoFn<byte[], KV<String, Integer>> {
    @ProcessElement
    public void processElement(OutputReceiver<KV<String, Integer>> out) {
      // Window [0, 10): a=1, a=2, b=5.
      out.outputWithTimestamp(KV.of("a", 1), new Instant(1));
      out.outputWithTimestamp(KV.of("a", 2), new Instant(2));
      out.outputWithTimestamp(KV.of("b", 5), new Instant(3));
      // Window [10, 20): a=3.
      out.outputWithTimestamp(KV.of("a", 3), new Instant(15));
    }
  }

  /** Records each grouped result as {@code "key=[sorted values]"}. */
  private static class RecordGroupFn extends DoFn<KV<String, Iterable<Integer>>, Void> {
    private final SharedTestCollector<String> collector;

    RecordGroupFn(SharedTestCollector<String> collector) {
      this.collector = collector;
    }

    @ProcessElement
    public void processElement(@Element KV<String, Iterable<Integer>> group) {
      List<Integer> values = new ArrayList<>();
      group.getValue().forEach(values::add);
      Collections.sort(values);
      collector.record(group.getKey() + "=" + values);
    }
  }

  @Test
  public void groupsValuesPerFixedWindow() {
    try (SharedTestCollector<String> collector = SharedTestCollector.create()) {
      Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
      pipeline
          .apply("impulse", Impulse.create())
          .apply("emit", ParDo.of(new EmitTimestampedKvsFn()))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
          .apply("window", Window.into(FixedWindows.of(WINDOW_SIZE)))
          .apply("gbk", GroupByKey.create())
          .apply("record", ParDo.of(new RecordGroupFn(collector)));

      KafkaStreamsTestRunner.run(pipeline);

      List<String> groups = collector.recorded();
      // a splits across two windows -> two groups; b has one; three groups total.
      assertThat(groups.size(), is(3));
      assertThat(groups, hasItems("a=[1, 2]", "a=[3]", "b=[5]"));
    }
  }
}
