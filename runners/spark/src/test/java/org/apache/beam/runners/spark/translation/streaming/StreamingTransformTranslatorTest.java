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
package org.apache.beam.runners.spark.translation.streaming;

import org.apache.beam.runners.spark.PipelineRule;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test class for StreamingTransformTranslator.
 */
public class StreamingTransformTranslatorTest {
  @Rule public final transient PipelineRule pipelineRule = PipelineRule.streaming();

  @Test
  public void testEvaluationGroupByKeyInStreamingMode() {
    Pipeline pipeline = pipelineRule.createPipeline();
    Instant instant = new Instant(0);
    CreateStream<Integer> source =
        CreateStream.of(VarIntCoder.of(), pipelineRule.batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(6)))
            .nextBatch(
                TimestampedValue.of(1, instant),
                TimestampedValue.of(2, instant),
                TimestampedValue.of(3, instant))
            .advanceNextBatchWatermarkToInfinity();

    PCollection<Integer> windowed =
        pipeline
            .apply(source)
            .apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(5))));
    PCollectionView<Integer> max =
        windowed.apply(Max.integersGlobally().withFanout(5).asSingletonView());
    pipeline.run();
  }
}
