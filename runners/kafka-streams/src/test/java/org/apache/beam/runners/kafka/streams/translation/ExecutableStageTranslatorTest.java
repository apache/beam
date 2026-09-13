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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import org.apache.beam.runners.kafka.streams.KafkaStreamsTestRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Test;

/**
 * End-to-end test for {@link ExecutableStageTranslator}: builds an {@code Impulse -> ParDo}
 * pipeline with the high-level Beam Java SDK and runs it through {@link KafkaStreamsTestRunner}.
 * The fused ParDo executes in an in-process (EMBEDDED) Java SDK harness, so the {@link DoFn}'s
 * {@code @ProcessElement} body runs for real — no Docker, no broker.
 *
 * <p>Because the ParDo's output PCollection has no downstream consumer, it is not a stage output
 * and is never forwarded out of the harness — that is the documented behaviour. The test verifies
 * the bridge works by having the DoFn record into a {@link SharedTestCollector} as a side effect
 * and asserting the recorded input from the test thread.
 */
public class ExecutableStageTranslatorTest {

  /**
   * Records the length of every input element seen by the harness so the test can verify the DoFn
   * ran. {@link SharedTestCollector} carries its identity via a UUID stored on the instance itself,
   * so it survives any serialization the runner may perform on the DoFn.
   */
  private static class RecordingFn extends DoFn<byte[], byte[]> {
    private final SharedTestCollector<Integer> collector;

    RecordingFn(SharedTestCollector<Integer> collector) {
      this.collector = collector;
    }

    @ProcessElement
    public void processElement(@Element byte[] input, OutputReceiver<byte[]> out) {
      collector.record(input.length);
      // Still emit something so the output codepath of the harness is exercised, even though no
      // downstream consumer means the runner never observes the value.
      out.output(new byte[] {1});
    }
  }

  @Test
  public void impulseThenParDoExecutesDoFnInHarnessOncePerImpulseElement() {
    try (SharedTestCollector<Integer> collector = SharedTestCollector.create()) {
      Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
      pipeline
          .apply("impulse", Impulse.create())
          .apply("pardo", ParDo.of(new RecordingFn(collector)));

      KafkaStreamsTestRunner.run(pipeline);

      List<Integer> recorded = collector.recorded();
      // Impulse emits exactly one empty byte[] in the GlobalWindow, so the DoFn must run exactly
      // once and see a zero-length input.
      assertThat(recorded.size(), is(1));
      assertThat(recorded.get(0), is(0));
    }
  }
}
