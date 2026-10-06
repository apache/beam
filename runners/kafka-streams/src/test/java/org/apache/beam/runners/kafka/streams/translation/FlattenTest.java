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
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.Test;

/**
 * End-to-end test for {@link FlattenTranslator}: two branches are flattened into one PCollection
 * and a recording ParDo sees every element from both.
 *
 * <p>Each branch is a {@code Create -> identity ParDo}, so its producer feeding the Flatten is an
 * {@link ExecutableStageProcessor} — the same shape PAssert's {@code GroupGlobally} produces. This
 * exercises the per-producing-transform watermark aggregation: each branch's producer stamps its
 * own transform id on its watermark, and the Flatten holds its output watermark until every
 * upstream transform it expects has reported. Without that, the Flatten would release its watermark
 * after the first branch drained and the downstream stage's bundle would close early, dropping the
 * second branch's elements.
 */
public class FlattenTest {

  private static class IdentityFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(@Element Integer input, OutputReceiver<Integer> out) {
      out.output(input);
    }
  }

  /** Records every element the harness feeds it so the test can assert the flatten's union. */
  private static class RecordingFn extends DoFn<Integer, Integer> {
    private final SharedTestCollector<Integer> collector;

    RecordingFn(SharedTestCollector<Integer> collector) {
      this.collector = collector;
    }

    @ProcessElement
    public void processElement(@Element Integer input, OutputReceiver<Integer> out) {
      collector.record(input);
      out.output(input);
    }
  }

  @Test
  public void flattenUnionsEveryBranch() {
    try (SharedTestCollector<Integer> collector = SharedTestCollector.create()) {
      Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
      PCollection<Integer> a =
          pipeline.apply("createA", Create.of(1, 2)).apply("idA", ParDo.of(new IdentityFn()));
      PCollection<Integer> b =
          pipeline.apply("createB", Create.of(3, 4)).apply("idB", ParDo.of(new IdentityFn()));
      PCollectionList.of(a)
          .and(b)
          .apply("flatten", Flatten.pCollections())
          .apply("record", ParDo.of(new RecordingFn(collector)));

      KafkaStreamsTestRunner.run(pipeline);

      List<Integer> recorded = collector.recorded();
      assertThat(recorded.size(), is(4));
      assertThat(recorded, hasItems(1, 2, 3, 4));
    }
  }

  @Test
  public void pCollectionFeedingTwoFlattensIsSupported() {
    // input2 feeds both flattens, so its producer's watermark report is consumed by two different
    // aggregators. The producer stamps its own transform id once, and each flatten holds until its
    // own two branches drain. Verify both flattens produce the right union.
    try (SharedTestCollector<Integer> left = SharedTestCollector.create();
        SharedTestCollector<Integer> right = SharedTestCollector.create()) {
      Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
      PCollection<Integer> input1 =
          pipeline.apply("c1", Create.of(1, 2)).apply("id1", ParDo.of(new IdentityFn()));
      PCollection<Integer> input2 =
          pipeline.apply("c2", Create.of(3, 4)).apply("id2", ParDo.of(new IdentityFn()));
      PCollection<Integer> input3 =
          pipeline.apply("c3", Create.of(5, 6)).apply("id3", ParDo.of(new IdentityFn()));
      PCollectionList.of(input1)
          .and(input2)
          .apply("l1", Flatten.pCollections())
          .apply("recordL1", ParDo.of(new RecordingFn(left)));
      PCollectionList.of(input2)
          .and(input3)
          .apply("l2", Flatten.pCollections())
          .apply("recordL2", ParDo.of(new RecordingFn(right)));

      KafkaStreamsTestRunner.run(pipeline);

      assertThat(left.recorded().size(), is(4));
      assertThat(left.recorded(), hasItems(1, 2, 3, 4));
      assertThat(right.recorded().size(), is(4));
      assertThat(right.recorded(), hasItems(3, 4, 5, 6));
    }
  }

  /** Maps each int to {@code KV("k", int)} so a downstream GroupByKey groups all branches. */
  private static class ToKvFn extends DoFn<Integer, KV<String, Integer>> {
    @ProcessElement
    public void processElement(@Element Integer input, OutputReceiver<KV<String, Integer>> out) {
      out.output(KV.of("k", input));
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
  public void watermarkPropagatesThroughFlattenAndFiresDownstreamGroupByKey() {
    // GroupByKey fires exactly once, when its input watermark reaches the end of the global
    // window, and the Flatten forwards its watermark only after every branch has drained. Both
    // branches share the key, so a single group holding the elements of both branches proves the
    // watermark propagated through the Flatten at the right time — a premature release would fire
    // a partial group instead.
    try (SharedTestCollector<String> collector = SharedTestCollector.create()) {
      Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
      PCollection<KV<String, Integer>> a =
          pipeline
              .apply("createA", Create.of(1, 2))
              .apply("kvA", ParDo.of(new ToKvFn()))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
      PCollection<KV<String, Integer>> b =
          pipeline
              .apply("createB", Create.of(3, 4))
              .apply("kvB", ParDo.of(new ToKvFn()))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
      PCollectionList.of(a)
          .and(b)
          .apply("flatten", Flatten.pCollections())
          .apply("gbk", GroupByKey.create())
          .apply("record", ParDo.of(new RecordGroupFn(collector)));

      KafkaStreamsTestRunner.run(pipeline);

      List<String> groups = collector.recorded();
      assertThat(groups.size(), is(1));
      assertThat(groups, hasItems("k=[1, 2, 3, 4]"));
    }
  }

  @Test
  public void flattenOfOneBranchTwiceDuplicatesEveryElement() {
    // A self-flatten is a bag union with itself: every element must appear twice. The fuser folds
    // the Flatten into the SDK-harness stage (it never reaches the runner's Flatten translator as a
    // duplicate-input node), so the duplication happens in the harness.
    try (SharedTestCollector<Integer> collector = SharedTestCollector.create()) {
      Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
      PCollection<Integer> branch =
          pipeline.apply("create", Create.of(1, 2)).apply("id", ParDo.of(new IdentityFn()));
      PCollectionList.of(branch)
          .and(branch)
          .apply("flatten", Flatten.pCollections())
          .apply("record", ParDo.of(new RecordingFn(collector)));

      KafkaStreamsTestRunner.run(pipeline);

      List<Integer> recorded = collector.recorded();
      assertThat(recorded.size(), is(4));
      assertThat(recorded, hasItems(1, 2));
      assertThat(recorded.stream().filter(v -> v == 1).count(), is(2L));
      assertThat(recorded.stream().filter(v -> v == 2).count(), is(2L));
    }
  }
}
