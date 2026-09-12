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
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

/**
 * End-to-end test of GroupByKey: {@code Impulse -> emit KVs -> GroupByKey -> record groups}.
 *
 * <p>Driven through {@link KafkaStreamsTestRunner}, which round-trips the internal repartition
 * topic that GroupByKey introduces. The downstream {@code RecordGroupFn} records each emitted group
 * into a {@link SharedTestCollector}.
 */
public class GroupByKeyTest {

  /** Emits a few KVs from the single impulse element so there is something to group. */
  private static class EmitKvsFn extends DoFn<byte[], KV<String, Integer>> {
    @ProcessElement
    public void processElement(OutputReceiver<KV<String, Integer>> out) {
      out.output(KV.of("a", 1));
      out.output(KV.of("a", 2));
      out.output(KV.of("b", 3));
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
  public void groupsValuesByKeyAndFiresAtWatermark() {
    try (SharedTestCollector<String> collector = SharedTestCollector.create()) {
      Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
      pipeline
          .apply("impulse", Impulse.create())
          .apply("emit", ParDo.of(new EmitKvsFn()))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
          .apply("gbk", GroupByKey.create())
          .apply("record", ParDo.of(new RecordGroupFn(collector)));

      KafkaStreamsTestRunner.run(pipeline);

      List<String> groups = collector.recorded();
      assertThat(groups.size(), is(2));
      assertThat(groups, hasItems("a=[1, 2]", "b=[3]"));
    }
  }
}
