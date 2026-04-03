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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowedValues;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MetadataPropagationTest {

  /** Tests for metadata propagation. */
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  static class CausedByDrainSettingDoFn extends DoFn<Integer, String> {
    @ProcessElement
    public void process(OutputReceiver<String> r) {
      r.builder("value").setCausedByDrain(CausedByDrain.CAUSED_BY_DRAIN).output();
    }
  }

  static class CausedByDrainExtractingDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext pc, OutputReceiver<String> r) {
      r.output(pc.causedByDrain().toString());
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMetadataPropagationAcrossShuffleParameter() {
    WindowedValues.WindowedValueCoder.setMetadataSupported();
    PCollection<String> results =
        pipeline
            .apply(Create.of(1))
            .apply(ParDo.of(new CausedByDrainSettingDoFn()))
            .apply(Redistribute.arbitrarily())
            .apply(ParDo.of(new CausedByDrainExtractingDoFn()));

    PAssert.that(results).containsInAnyOrder("CAUSED_BY_DRAIN");

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMetadataPropagationParameter() {
    PCollection<String> results =
        pipeline
            .apply(Create.of(1))
            .apply(ParDo.of(new CausedByDrainSettingDoFn()))
            .apply(ParDo.of(new CausedByDrainExtractingDoFn()));

    PAssert.that(results).containsInAnyOrder("CAUSED_BY_DRAIN");

    pipeline.run();
  }
}
