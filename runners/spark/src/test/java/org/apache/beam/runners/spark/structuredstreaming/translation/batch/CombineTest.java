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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for beam to spark {@link org.apache.beam.sdk.transforms.Combine} translation. */
@RunWith(JUnit4.class)
public class CombineTest implements Serializable {
  private static Pipeline p;

  @BeforeClass
  public static void beforeClass() {
    PipelineOptions options = PipelineOptionsFactory.create().as(PipelineOptions.class);
    options.setRunner(SparkStructuredStreamingRunner.class);
    p = Pipeline.create(options);
  }

  @Ignore
  @Test
  public void testCombineGlobally() {
    PCollection<Integer> input =
        p.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).apply(Sum.integersGlobally());
    PAssert.that(input).containsInAnyOrder(55);
    // uses combine per key
    p.run();
  }

  @Test
  public void testCombinePerKey() {
    List<KV<Integer, Integer>> elems = new ArrayList<>();
    elems.add(KV.of(1, 1));
    elems.add(KV.of(1, 3));
    elems.add(KV.of(1, 5));
    elems.add(KV.of(2, 2));
    elems.add(KV.of(2, 4));
    elems.add(KV.of(2, 6));

    PCollection<KV<Integer, Integer>> input = p.apply(Create.of(elems)).apply(Sum.integersPerKey());
    PAssert.that(input).containsInAnyOrder(KV.of(1, 9), KV.of(2, 12));
    p.run();
  }

  @Test
  public void testCombinePerKeyPreservesWindowing(){
    PCollection<KV<Integer, Integer>> input = p.apply(Create
        .timestamped(TimestampedValue.of(KV.of(1, 1), new Instant(1)),
            TimestampedValue.of(KV.of(1, 3), new Instant(2)),
            TimestampedValue.of(KV.of(1, 5), new Instant(11)),
            TimestampedValue.of(KV.of(2, 2), new Instant(3)),
            TimestampedValue.of(KV.of(2, 4), new Instant(11)),
            TimestampedValue.of(KV.of(2, 6), new Instant(12))))
        .apply(Window.into(FixedWindows.of(Duration.millis(10)))).apply(Sum.integersPerKey());
    PAssert.that(input).containsInAnyOrder(KV.of(1, 4), KV.of(1, 5), KV.of(2, 2), KV.of(2, 10));
    p.run();

  }

}
