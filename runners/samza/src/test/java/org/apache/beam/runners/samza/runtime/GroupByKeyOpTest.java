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
package org.apache.beam.runners.samza.runtime;

import static org.junit.Assume.assumeTrue;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/** Tests for GroupByKeyOp. */
public class GroupByKeyOpTest implements Serializable {

  @BeforeClass
  public static void beforeClass() {
    // TODO(https://github.com/apache/beam/issues/32208)
    assumeTrue(System.getProperty("java.version").startsWith("1."));
  }

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.fromOptions(
          PipelineOptionsFactory.fromArgs("--runner=TestSamzaRunner").create());

  @Rule
  public final transient TestPipeline dropLateDataPipeline =
      TestPipeline.fromOptions(
          PipelineOptionsFactory.fromArgs("--runner=TestSamzaRunner", "--dropLateData=true")
              .create());

  @Test
  public void testDefaultGbk() {
    TestStream.Builder<Integer> testStream =
        TestStream.create(VarIntCoder.of())
            .addElements(TimestampedValue.of(1, new Instant(1000)))
            .addElements(TimestampedValue.of(2, new Instant(2000)))
            .advanceWatermarkTo(new Instant(3000))
            .addElements(TimestampedValue.of(10, new Instant(1000)))
            .advanceWatermarkTo(new Instant(10000));

    PCollection<Integer> aggregated =
        pipeline
            .apply(testStream.advanceWatermarkToInfinity())
            .apply(
                Window.<Integer>into(FixedWindows.of(Duration.standardSeconds(3)))
                    .accumulatingFiredPanes())
            .apply(Combine.globally(Sum.ofIntegers()).withoutDefaults());

    PAssert.that(aggregated).containsInAnyOrder(Arrays.asList(3, 10));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testDropLateDataNonKeyed() {
    TestStream.Builder<Integer> testStream =
        TestStream.create(VarIntCoder.of())
            .addElements(TimestampedValue.of(1, new Instant(1000)))
            .addElements(TimestampedValue.of(2, new Instant(2000)))
            .advanceWatermarkTo(new Instant(3000))
            .addElements(TimestampedValue.of(10, new Instant(1000)))
            .advanceWatermarkTo(new Instant(10000));

    PCollection<Integer> aggregated =
        dropLateDataPipeline
            .apply(testStream.advanceWatermarkToInfinity())
            .apply(
                Window.<Integer>into(FixedWindows.of(Duration.standardSeconds(3)))
                    .accumulatingFiredPanes())
            .apply(Combine.globally(Sum.ofIntegers()).withoutDefaults());

    PAssert.that(aggregated).containsInAnyOrder(3);

    dropLateDataPipeline.run().waitUntilFinish();
  }

  @Test
  public void testDropLateDataKeyed() {
    TestStream.Builder<KV<String, Integer>> testStream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
            .addElements(TimestampedValue.of(KV.of("a", 1), new Instant(1000)))
            .addElements(TimestampedValue.of(KV.of("b", 2), new Instant(2000)))
            .addElements(TimestampedValue.of(KV.of("a", 3), new Instant(2500)))
            .advanceWatermarkTo(new Instant(3000))
            .addElements(TimestampedValue.of(KV.of("a", 10), new Instant(1000)))
            .advanceWatermarkTo(new Instant(10000));

    PCollection<KV<String, Integer>> aggregated =
        dropLateDataPipeline
            .apply(testStream.advanceWatermarkToInfinity())
            .apply(
                Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardSeconds(3)))
                    .accumulatingFiredPanes())
            .apply(Sum.integersPerKey());

    PAssert.that(aggregated).containsInAnyOrder(Arrays.asList(KV.of("a", 4), KV.of("b", 2)));

    dropLateDataPipeline.run().waitUntilFinish();
  }
}
