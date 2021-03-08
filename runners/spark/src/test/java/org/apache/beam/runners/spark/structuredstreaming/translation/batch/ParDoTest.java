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
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for beam to spark {@link ParDo} translation. */
@RunWith(JUnit4.class)
public class ParDoTest implements Serializable {
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
  public void testPardo() {
    PCollection<Integer> input =
        pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).apply(ParDo.of(PLUS_ONE_DOFN));
    PAssert.that(input).containsInAnyOrder(2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
    pipeline.run();
  }

  @Test
  public void testTwoPardoInRow() {
    PCollection<Integer> input =
        pipeline
            .apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
            .apply(ParDo.of(PLUS_ONE_DOFN))
            .apply(ParDo.of(PLUS_ONE_DOFN));
    PAssert.that(input).containsInAnyOrder(3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    pipeline.run();
  }

  @Test
  public void testSideInputAsList() {
    PCollectionView<List<Integer>> sideInputView =
        pipeline.apply("Create sideInput", Create.of(1, 2, 3)).apply(View.asList());
    PCollection<Integer> input =
        pipeline
            .apply("Create input", Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
            .apply(
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            List<Integer> sideInputValue = c.sideInput(sideInputView);
                            if (!sideInputValue.contains(c.element())) {
                              c.output(c.element());
                            }
                          }
                        })
                    .withSideInputs(sideInputView));
    PAssert.that(input).containsInAnyOrder(4, 5, 6, 7, 8, 9, 10);
    pipeline.run();
  }

  @Test
  public void testSideInputAsSingleton() {
    PCollectionView<Integer> sideInputView =
        pipeline.apply("Create sideInput", Create.of(1)).apply(View.asSingleton());

    PCollection<Integer> input =
        pipeline
            .apply("Create input", Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
            .apply(
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            Integer sideInputValue = c.sideInput(sideInputView);
                            if (!sideInputValue.equals(c.element())) {
                              c.output(c.element());
                            }
                          }
                        })
                    .withSideInputs(sideInputView));

    PAssert.that(input).containsInAnyOrder(2, 3, 4, 5, 6, 7, 8, 9, 10);
    pipeline.run();
  }

  @Test
  public void testSideInputAsMap() {
    PCollectionView<Map<String, Integer>> sideInputView =
        pipeline
            .apply("Create sideInput", Create.of(KV.of("key1", 1), KV.of("key2", 2)))
            .apply(View.asMap());
    PCollection<Integer> input =
        pipeline
            .apply("Create input", Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
            .apply(
                ParDo.of(
                        new DoFn<Integer, Integer>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            Map<String, Integer> sideInputValue = c.sideInput(sideInputView);
                            if (!sideInputValue.containsKey("key" + c.element())) {
                              c.output(c.element());
                            }
                          }
                        })
                    .withSideInputs(sideInputView));
    PAssert.that(input).containsInAnyOrder(3, 4, 5, 6, 7, 8, 9, 10);
    pipeline.run();
  }

  private static final DoFn<Integer, Integer> PLUS_ONE_DOFN =
      new DoFn<Integer, Integer>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          c.output(c.element() + 1);
        }
      };
}
