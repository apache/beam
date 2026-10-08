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
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Test;

/**
 * End-to-end test for {@link ReadTranslator}: a primitive bounded {@code Read} over a {@link
 * CountingSource} feeds a recording ParDo executed in the in-process (EMBEDDED) Java SDK harness.
 * Verifies every source element reaches the DoFn.
 *
 * <p>{@code KafkaStreamsTestRunner.translate} forces the {@code Read.Bounded} into the deprecated
 * primitive read the runner translates, rather than the default splittable-DoFn expansion — so this
 * exercises the {@link ReadProcessor} + {@link ReadTranslator} path end to end.
 */
public class ReadTest {

  /** Records every element the harness feeds it so the test can assert the read produced them. */
  private static class RecordingFn extends DoFn<Long, Long> {
    private final SharedTestCollector<Long> collector;

    RecordingFn(SharedTestCollector<Long> collector) {
      this.collector = collector;
    }

    @ProcessElement
    public void processElement(@Element Long input, OutputReceiver<Long> out) {
      collector.record(input);
      out.output(input);
    }
  }

  @Test
  public void boundedReadEmitsEverySourceElement() {
    try (SharedTestCollector<Long> collector = SharedTestCollector.create()) {
      Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
      pipeline
          .apply("read", Read.from(CountingSource.upTo(5)))
          .apply("record", ParDo.of(new RecordingFn(collector)));

      KafkaStreamsTestRunner.run(pipeline);

      List<Long> recorded = collector.recorded();
      // CountingSource.upTo(5) yields 0..4 exactly once each.
      assertThat(recorded.size(), is(5));
      assertThat(recorded, hasItems(0L, 1L, 2L, 3L, 4L));
    }
  }
}
