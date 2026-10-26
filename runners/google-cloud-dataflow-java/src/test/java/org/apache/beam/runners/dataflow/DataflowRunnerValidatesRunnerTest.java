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
package org.apache.beam.runners.dataflow;

import static org.junit.Assume.assumeFalse;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.BeamParallelJunit4Runner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValues;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * {@link ValidatesRunner} tests for {@link DataflowRunner} that execute a pipeline on the service.
 *
 * <p>These live apart from {@link DataflowRunnerTest} so that they can run under {@link
 * BeamParallelJunit4Runner} (and thus concurrently, and merged into a single Dataflow job by {@link
 * TestDataflowRunner}) while the unit tests in {@link DataflowRunnerTest}, some of which rely on
 * process-wide state such as log capture, keep running serially under plain JUnit4.
 *
 * <p>{@link Serializable} because the anonymous {@link TupleTag}s and {@link DoFn}s below capture
 * the enclosing instance.
 */
@RunWith(BeamParallelJunit4Runner.class)
public class DataflowRunnerValidatesRunnerTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category({ValidatesRunner.class, UsesStatefulParDo.class})
  public void testBatchGroupIntoBatchesOverrideCount() {
    // Ignore this test for streaming pipelines.
    assumeFalse(pipeline.getOptions().as(StreamingOptions.class).isStreaming());
    DataflowRunnerTest.verifyGroupIntoBatchesOverrideCount(pipeline, false, true);
  }

  @Test
  @Category({ValidatesRunner.class, UsesStatefulParDo.class})
  public void testBatchGroupIntoBatchesOverrideBytes() {
    // Ignore this test for streaming pipelines.
    assumeFalse(pipeline.getOptions().as(StreamingOptions.class).isStreaming());
    DataflowRunnerTest.verifyGroupIntoBatchesOverrideBytes(pipeline, false, true);
  }

  @Test
  @Category({ValidatesRunner.class})
  public void testValueKindParameterAndOutputWithKind() {
    boolean isRunnerV2 = false;
    @Nullable List<String> experiments =
        pipeline.getOptions().as(DataflowPipelineOptions.class).getExperiments();
    if (experiments != null
        && (experiments.contains("use_unified_worker") || experiments.contains("use_runner_v2"))) {
      isRunnerV2 = true;
    }
    // Skipp runner v2 because its Create uses a splittable DoFn, which contains a shuffle.
    // ValueKind is not supported in Dataflow shuffle yet
    assumeFalse(isRunnerV2);

    PCollection<String> input = pipeline.apply(Create.of("a", "b", "c", "d"));
    TupleTag<String> mainTag = new TupleTag<String>() {};
    TupleTag<String> sideTag = new TupleTag<String>() {};

    PCollectionTuple tuple =
        input.apply(
            "SetKind",
            ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element,
                          @Timestamp org.joda.time.Instant timestamp,
                          BoundedWindow window,
                          PaneInfo paneInfo,
                          ProcessContext c,
                          MultiOutputReceiver outputReceiver) {
                        switch (element) {
                          case "a":
                            c.output(element); // default: INSERT
                            return;
                          case "b":
                            c.outputWindowedValue(
                                WindowedValues.of(
                                    element,
                                    timestamp,
                                    Collections.singleton(window),
                                    paneInfo,
                                    null,
                                    null,
                                    CausedByDrain.NORMAL,
                                    null,
                                    ValueKind.UPDATE_BEFORE));
                            return;
                          case "c":
                            outputReceiver
                                .get(mainTag)
                                .builder(element)
                                .setValueKind(ValueKind.UPDATE_AFTER)
                                .output();
                            return;
                          case "d":
                            outputReceiver
                                .get(sideTag)
                                .builder(element)
                                .setValueKind(ValueKind.DELETE)
                                .output();
                        }
                      }
                    })
                .withOutputTags(mainTag, TupleTagList.of(sideTag)));

    PCollection<String> main =
        tuple
            .get(mainTag)
            .apply(
                "ReadKind",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element, ProcessContext c, ValueKind kind) {
                        c.output(element + ":" + kind);
                      }
                    }));

    PCollection<String> side =
        tuple
            .get(sideTag)
            .apply(
                "ReadKind-SideTag",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element String element, ProcessContext c, ValueKind kind) {
                        c.output(element + ":" + kind);
                      }
                    }));

    PAssert.that(main).containsInAnyOrder("a:INSERT", "b:UPDATE_BEFORE", "c:UPDATE_AFTER");
    PAssert.that(side).containsInAnyOrder("d:DELETE");
    pipeline.run();
  }

  @Test
  @Ignore("enable once when element metadata is supported in shuffle")
  @Category({ValidatesRunner.class})
  public void testValueKindPreservedAcrossShuffle() {
    PCollection<KV<String, String>> input = pipeline.apply(Create.of(KV.of("key", "value")));

    PCollection<String> output =
        input
            .apply(
                "SetKind",
                ParDo.of(
                    new DoFn<KV<String, String>, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, String> element,
                          OutputReceiver<KV<String, String>> out) {
                        out.builder(element).setValueKind(ValueKind.UPDATE_BEFORE).output();
                      }
                    }))
            .apply(Reshuffle.of())
            .apply(
                "ReadKind",
                ParDo.of(
                    new DoFn<KV<String, String>, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, String> element, ProcessContext c, ValueKind kind) {
                        c.output(element.getValue() + ":" + kind);
                      }
                    }));

    PAssert.that(output).containsInAnyOrder("value:UPDATE_BEFORE");
    pipeline.run();
  }
}
