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

import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
    PipelineOptions options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    pipeline = Pipeline.create(options);
  }

  @Test
  public void testPardo() {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    input.apply(
        ParDo.of(
            new DoFn<Integer, Integer>() {
              @ProcessElement
              public void processElement(ProcessContext context) {
                context.output(context.element() + 1);
              }
            }));
    pipeline.run();
  }

  @Test
  public void testTwoPardoInRow() {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    input
        .apply(
            ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    Integer val = context.element() + 1;
                    context.output(val);
                    System.out.println("ParDo1: val = " + val);
                  }
                }))
        .apply(
            ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    Integer val = context.element() + 1;
                    context.output(val);
                    System.out.println("ParDo2: val = " + val);
                  }
                }));
    pipeline.run();
  }

  @Test
  public void testSideInputAsList() {
    PCollection<Integer> sideInput = pipeline.apply("Create sideInput", Create.of(101, 102, 103));
    final PCollectionView<List<Integer>> sideInputView = sideInput.apply(View.asList());

    PCollection<Integer> input =
        pipeline.apply("Create input", Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    input.apply(
        ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    List<Integer> sideInputValue = context.sideInput(sideInputView);
                    Integer val = context.element();
                    context.output(val);
                    System.out.println(
                        "ParDo1: val = " + val + ", sideInputValue = " + sideInputValue);
                  }
                })
            .withSideInputs(sideInputView));

    pipeline.run();
  }

  @Test
  public void testSideInputAsSingleton() {
    PCollection<Integer> sideInput = pipeline.apply("Create sideInput", Create.of(101));
    final PCollectionView<Integer> sideInputView = sideInput.apply(View.asSingleton());

    PCollection<Integer> input =
        pipeline.apply("Create input", Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    input.apply(
        ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    Integer sideInputValue = context.sideInput(sideInputView);
                    Integer val = context.element();
                    context.output(val);
                    System.out.println(
                        "ParDo1: val = " + val + ", sideInputValue = " + sideInputValue);
                  }
                })
            .withSideInputs(sideInputView));

    pipeline.run();
  }

  @Test
  public void testSideInputAsMap() {
    PCollection<KV<String, Integer>> sideInput =
        pipeline.apply("Create sideInput", Create.of(KV.of("key1", 1), KV.of("key2", 2)));
    final PCollectionView<Map<String, Integer>> sideInputView = sideInput.apply(View.asMap());

    PCollection<Integer> input =
        pipeline.apply("Create input", Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    input.apply(
        ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    Map<String, Integer> sideInputValue = context.sideInput(sideInputView);
                    Integer val = context.element();
                    context.output(val);
                    System.out.println(
                        "ParDo1: val = " + val + ", sideInputValue = " + sideInputValue);
                  }
                })
            .withSideInputs(sideInputView));

    pipeline.run();
  }
}
