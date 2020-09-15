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
package org.apache.beam.runners.flink.streaming;

import static org.junit.Assert.assertNull;

import java.io.Serializable;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.TestFlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests grouping with null values. */
public class GroupByWithNullValuesTest implements Serializable {

  @Test
  public void testGroupByWithNullValues() {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);

    options.setRunner(TestFlinkRunner.class);
    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);
    PCollection<Integer> result =
        pipeline
            .apply(
                GenerateSequence.from(0)
                    .to(100)
                    .withTimestampFn(
                        new SerializableFunction<Long, Instant>() {
                          @Override
                          public Instant apply(Long input) {
                            return new Instant(input);
                          }
                        }))
            .apply(Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(
                ParDo.of(
                    new DoFn<Long, KV<String, Void>>() {
                      @ProcessElement
                      public void processElement(ProcessContext pc) {
                        pc.output(KV.of("hello", null));
                      }
                    }))
            .apply(GroupByKey.create())
            .apply(
                ParDo.of(
                    new DoFn<KV<String, Iterable<Void>>, Integer>() {
                      @ProcessElement
                      public void processElement(ProcessContext pc) {
                        int count = 0;
                        for (Void aVoid : pc.element().getValue()) {
                          assertNull("Element should be null", aVoid);
                          count++;
                        }
                        pc.output(count);
                      }
                    }));

    PAssert.that(result).containsInAnyOrder(10, 10, 10, 10, 10, 10, 10, 10, 10, 10);

    pipeline.run();
  }
}
