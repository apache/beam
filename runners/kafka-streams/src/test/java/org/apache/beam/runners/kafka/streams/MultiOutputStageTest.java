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
package org.apache.beam.runners.kafka.streams;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

/**
 * Verifies that a fused executable stage with more than one output routes each output to its own
 * downstream — including the case the mentor asked about, where each output is followed by a
 * separate GroupByKey (so each output ends up in its own repartition topic).
 *
 * <p>A DoFn splits its input to a main and an additional (side) output. One output is grouped with
 * {@code Count.perElement} (a GroupByKey under the hood, so that output lands in its own
 * repartition topic); the other is asserted directly (a plain in-process output). A {@link PAssert}
 * on each confirms the two outputs carried the right elements to the right downstream.
 */
public class MultiOutputStageTest {

  private static final TupleTag<Integer> EVENS = new TupleTag<Integer>() {};
  private static final TupleTag<Integer> ODDS = new TupleTag<Integer>() {};

  /** Routes even elements to the main output and odd elements to the side output. */
  private static class SplitByParityFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(@Element Integer input, MultiOutputReceiver out) {
      if (input % 2 == 0) {
        out.get(EVENS).output(input);
      } else {
        out.get(ODDS).output(input);
      }
    }
  }

  private static PipelineOptions options() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(TestKafkaStreamsRunner.class);
    return options;
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.fromOptions(options());

  @Test
  public void eachStageOutputFeedsItsOwnGroupByKey() {
    PCollectionTuple split =
        pipeline
            .apply("create", Create.of(1, 2, 3, 4, 5, 6))
            .apply(
                "split",
                ParDo.of(new SplitByParityFn()).withOutputTags(EVENS, TupleTagList.of(ODDS)));

    // The evens branch goes through a GroupByKey (Count.perElement), so that output lands in its
    // own repartition topic; the odds branch is asserted directly, exercising a plain output.
    PAssert.that(split.get(EVENS).apply("countEvens", Count.perElement()))
        .containsInAnyOrder(KV.of(2, 1L), KV.of(4, 1L), KV.of(6, 1L));
    PAssert.that(split.get(ODDS)).containsInAnyOrder(1, 3, 5);

    pipeline.run();
  }
}
