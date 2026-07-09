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

import java.util.List;
import org.apache.beam.runners.kafka.streams.KafkaStreamsTestRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.Test;

/**
 * End-to-end test for {@link FlattenTranslator}: two branches are flattened into one PCollection
 * and a recording ParDo sees every element from both.
 *
 * <p>Each branch is a {@code Create -> identity ParDo}, so its producer feeding the Flatten is an
 * {@link ExecutableStageProcessor} — the same shape PAssert's {@code GroupGlobally} produces. This
 * is the case the {@code (i of N)} watermark stamping exists for: without it both branches would
 * report as the single source {@code (0 of 1)}, the Flatten would release its watermark after the
 * first branch drained, and the downstream stage's bundle would close early and drop the second
 * branch's elements.
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
}
