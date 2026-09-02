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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafka.streams.KafkaStreamsTestRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that a bundle is closed once it reaches {@code --maxBundleSize}, rather than staying open
 * until the next watermark.
 *
 * <p>The bound is observable through the DoFn's own lifecycle: {@code @FinishBundle} runs once per
 * bundle the SDK harness processes, so feeding a known number of elements with a known bound tells
 * us how many bundles the stage actually opened.
 *
 * <p>The elements have to reach the stage as separate records for the bound to see them, since it
 * counts what is fed to the stage rather than what the user's code emits inside it. A DoFn that
 * fans one element out into many would be fused into the same stage and still be a single input, so
 * these pipelines read the elements from a {@link Create} instead — the runner translates that to a
 * primitive Read, which forwards one record per element.
 */
public class BundleBoundaryTest {

  private static final int ELEMENTS = 50;
  private static final int MAX_BUNDLE_SIZE = 10;

  /** Records how many times {@code @FinishBundle} fired, i.e. how many bundles were processed. */
  private static final List<String> FINISHED_BUNDLES =
      Collections.synchronizedList(new ArrayList<>());

  @Before
  public void resetCounters() {
    FINISHED_BUNDLES.clear();
  }

  /** Counts the bundles it is asked to process. */
  private static class CountBundlesFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(@Element Integer element, OutputReceiver<Integer> out) {
      out.output(element);
    }

    @FinishBundle
    public void finishBundle() {
      FINISHED_BUNDLES.add("bundle");
    }
  }

  private static List<Integer> elements() {
    List<Integer> elements = new ArrayList<>();
    for (int i = 0; i < ELEMENTS; i++) {
      elements.add(i);
    }
    return elements;
  }

  private static Pipeline buildPipeline(KafkaStreamsPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("read", Create.of(elements()))
        .apply("countBundles", ParDo.of(new CountBundlesFn()));
    return pipeline;
  }

  private static Pipeline pipelineWithBundleSize(int maxBundleSize) {
    KafkaStreamsPipelineOptions options =
        KafkaStreamsTestRunner.testOptions().as(KafkaStreamsPipelineOptions.class);
    options.setMaxBundleSize(maxBundleSize);
    return buildPipeline(options);
  }

  @Test
  public void aBundleIsClosedOnceItReachesTheSizeBound() {
    KafkaStreamsTestRunner.run(pipelineWithBundleSize(MAX_BUNDLE_SIZE));

    // 50 elements bounded at 10 cannot have gone through in fewer than 5 bundles. Without the
    // bound the whole run is one bundle, so this is what tells the two apart. The count is a lower
    // bound rather than exact: a watermark arriving mid-bundle also closes one.
    assertThat(FINISHED_BUNDLES.size(), is(greaterThanOrEqualTo(ELEMENTS / MAX_BUNDLE_SIZE)));
  }

  @Test
  public void aBoundLargerThanTheInputLeavesASingleBundle() {
    // The control: with a bound nothing reaches, the stage keeps one bundle open until the
    // terminal watermark closes it.
    KafkaStreamsTestRunner.run(pipelineWithBundleSize(ELEMENTS * 10));

    assertThat(FINISHED_BUNDLES.size(), is(1));
  }
}
