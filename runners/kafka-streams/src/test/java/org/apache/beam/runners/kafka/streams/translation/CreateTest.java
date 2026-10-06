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
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Test;

/**
 * End-to-end test that {@code Create} of two or more elements runs on the Kafka Streams runner.
 *
 * <p>{@code Create.of(1, 2, 3)} expands to {@code Read.from(CreateSource)}, a bounded {@code
 * Read.Bounded}. {@code KafkaStreamsTestRunner.translate} forces it into the deprecated primitive
 * read the runner translates (see {@link ReadTranslator}), so the elements flow through {@link
 * ReadProcessor} into a recording ParDo in the EMBEDDED harness. This is the multi-element Create
 * case that previously failed because the default expansion is an unsupported splittable DoFn.
 */
public class CreateTest {

  /** Records every element the harness feeds it so the test can assert Create produced them. */
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
  public void createOfMultipleElementsRunsThroughPrimitiveRead() {
    try (SharedTestCollector<Integer> collector = SharedTestCollector.create()) {
      Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
      pipeline
          .apply("create", Create.of(1, 2, 3))
          .apply("record", ParDo.of(new RecordingFn(collector)));

      KafkaStreamsTestRunner.run(pipeline);

      List<Integer> recorded = collector.recorded();
      assertThat(recorded.size(), is(3));
      assertThat(recorded, hasItems(1, 2, 3));
    }
  }
}
