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
package org.apache.beam.runners.flink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests {@link FlinkStreamingAggregationsTranslators}. */
@RunWith(JUnit4.class)
public class FlinkStreamingAggregationsTranslatorsTest {

  private static FlinkPipelineOptions options(boolean streaming, boolean sortedBatchCombine) {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setStreaming(streaming);
    options.setSortedBatchCombine(sortedBatchCombine);
    return options;
  }

  private static boolean eligible(
      FlinkPipelineOptions options,
      WindowingStrategy<?, ?> windowingStrategy,
      List<PCollectionView<?>> sideInputs) {
    return FlinkStreamingAggregationsTranslators.useSortedBatchCombine(
        options, windowingStrategy, sideInputs);
  }

  @Test
  public void usesSortedBatchCombineOnlyWhenEligible() {
    WindowingStrategy<?, ?> global = WindowingStrategy.globalDefault();
    List<PCollectionView<?>> none = Collections.emptyList();

    assertThat(eligible(options(false, true), global, none), is(true));

    assertThat("option off", eligible(options(false, false), global, none), is(false));
    assertThat("streaming", eligible(options(true, true), global, none), is(false));
    assertThat(
        "fixed windows",
        eligible(
            options(false, true),
            WindowingStrategy.of(FixedWindows.of(Duration.standardMinutes(1))),
            none),
        is(false));
    assertThat(
        "custom trigger",
        eligible(
            options(false, true),
            global.withTrigger(Repeatedly.forever(AfterPane.elementCountAtLeast(1))),
            none),
        is(false));

    TestPipeline pipeline = TestPipeline.create();
    PCollectionView<?> view = pipeline.apply(Create.of(1)).apply(View.asSingleton());
    assertThat(
        "side inputs",
        eligible(options(false, true), global, Collections.singletonList(view)),
        is(false));
  }
}
