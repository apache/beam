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

import static org.apache.beam.sdk.testing.SerializableMatchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for beam to spark {@link ParDo} translation. */
@RunWith(JUnit4.class)
public class GroupByKeyTest implements Serializable {
  private static Pipeline pipeline;

  @BeforeClass
  public static void beforeClass() {
    SparkStructuredStreamingPipelineOptions options =
        PipelineOptionsFactory.create().as(SparkStructuredStreamingPipelineOptions.class);
    options.setRunner(SparkStructuredStreamingRunner.class);
    options.setTestMode(true);
    pipeline = Pipeline.create(options);
  }

  @Test
  public void testGroupByKeyPreservesWindowing() {
    pipeline
        .apply(
            Create.timestamped(
                TimestampedValue.of(KV.of(1, 1), new Instant(1)),
                TimestampedValue.of(KV.of(1, 3), new Instant(2)),
                TimestampedValue.of(KV.of(1, 5), new Instant(11)),
                TimestampedValue.of(KV.of(2, 2), new Instant(3)),
                TimestampedValue.of(KV.of(2, 4), new Instant(11)),
                TimestampedValue.of(KV.of(2, 6), new Instant(12))))
        .apply(Window.into(FixedWindows.of(Duration.millis(10))))
        .apply(GroupByKey.create())
        // do manual assertion for windows because Passert do not support multiple kv with same key
        // (because multiple windows)
        .apply(
            ParDo.of(
                new DoFn<KV<Integer, Iterable<Integer>>, KV<Integer, Iterable<Integer>>>() {

                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    KV<Integer, Iterable<Integer>> element = context.element();
                    if (element.getKey() == 1) {
                      if (Iterables.size(element.getValue()) == 2) {
                        assertThat(element.getValue(), containsInAnyOrder(1, 3)); // window [0-10)
                      } else {
                        assertThat(element.getValue(), containsInAnyOrder(5)); // window [10-20)
                      }
                    } else { // key == 2
                      if (Iterables.size(element.getValue()) == 2) {
                        assertThat(element.getValue(), containsInAnyOrder(4, 6)); // window [10-20)
                      } else {
                        assertThat(element.getValue(), containsInAnyOrder(2)); // window [0-10)
                      }
                    }
                    context.output(element);
                  }
                }));
    pipeline.run();
  }

  @Test
  public void testGroupByKey() {
    List<KV<Integer, Integer>> elems = new ArrayList<>();
    elems.add(KV.of(1, 1));
    elems.add(KV.of(1, 3));
    elems.add(KV.of(1, 5));
    elems.add(KV.of(2, 2));
    elems.add(KV.of(2, 4));
    elems.add(KV.of(2, 6));

    PCollection<KV<Integer, Iterable<Integer>>> input =
        pipeline.apply(Create.of(elems)).apply(GroupByKey.create());
    PAssert.thatMap(input)
        .satisfies(
            results -> {
              assertThat(results.get(1), containsInAnyOrder(1, 3, 5));
              assertThat(results.get(2), containsInAnyOrder(2, 4, 6));
              return null;
            });
    pipeline.run();
  }
}
