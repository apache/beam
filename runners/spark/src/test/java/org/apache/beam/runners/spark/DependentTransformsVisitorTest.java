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
package org.apache.beam.runners.spark;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Objects;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests of {@link DependentTransformsVisitor}. */
public class DependentTransformsVisitorTest {

  @ClassRule public static SparkContextRule contextRule = new SparkContextRule();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testCountDependentTransformsOnApplyAndSideInputs() {
    SparkPipelineOptions options = contextRule.createPipelineOptions();
    Pipeline pipeline = Pipeline.create(options);
    PCollection<String> pCollection = pipeline.apply(Create.of("foo", "bar"));

    // First use of pCollection.
    PCollection<Long> leaf1 = pCollection.apply(Count.globally());
    // Second use of pCollection.
    PCollectionView<List<String>> view = pCollection.apply("yyy", View.asList());

    PCollection<String> leaf2 =
        pipeline
            .apply(Create.of("foo", "baz"))
            .apply(
                ParDo.of(
                        new DoFn<String, String>() {
                          @ProcessElement
                          public void processElement(ProcessContext processContext) {
                            if (processContext.sideInput(view).contains(processContext.element())) {
                              processContext.output(processContext.element());
                            }
                          }
                        })
                    .withSideInputs(view));

    EvaluationContext ctxt =
        new EvaluationContext(contextRule.getSparkContext(), pipeline, options);
    TransformTranslator.Translator translator = new TransformTranslator.Translator();
    pipeline.traverseTopologically(new DependentTransformsVisitor(translator, ctxt));

    assertEquals(2, ctxt.getPCollectionConsumptionMap().get(pCollection).intValue());
    assertEquals(0, ctxt.getPCollectionConsumptionMap().get(leaf1).intValue());
    assertEquals(0, ctxt.getPCollectionConsumptionMap().get(leaf2).intValue());
    assertEquals(2, ctxt.getPCollectionConsumptionMap().get(view.getPCollection()).intValue());
  }

  @Test
  public void testCountDependentTransformsOnSideOutputs() {
    SparkPipelineOptions options = contextRule.createPipelineOptions();
    Pipeline pipeline = Pipeline.create(options);

    TupleTag<String> passOutTag = new TupleTag<>("passOut");
    TupleTag<Long> lettersCountOutTag = new TupleTag<>("lettersOut");
    TupleTag<Long> wordCountOutTag = new TupleTag<>("wordsOut");

    PCollectionTuple result =
        pipeline
            .apply(Create.of("foo", "baz"))
            .apply(
                ParDo.of(
                        new DoFn<String, String>() {
                          @ProcessElement
                          public void processElement(ProcessContext processContext) {
                            String element = processContext.element();
                            processContext.output(element);
                            processContext.output(
                                lettersCountOutTag,
                                (long) Objects.requireNonNull(element).length());
                            processContext.output(wordCountOutTag, 1L);
                          }
                        })
                    .withOutputTags(
                        passOutTag,
                        TupleTagList.of(Lists.newArrayList(lettersCountOutTag, wordCountOutTag))));

    // consume main output and words side output. leave letters side output left alone
    result.get(wordCountOutTag).setCoder(VarLongCoder.of()).apply(Sum.longsGlobally());
    result.get(lettersCountOutTag).setCoder(VarLongCoder.of());
    result
        .get(passOutTag)
        .apply(
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext processContext) {
                    // do nothing
                  }
                }));

    EvaluationContext ctxt =
        new EvaluationContext(contextRule.getSparkContext(), pipeline, options);
    TransformTranslator.Translator translator = new TransformTranslator.Translator();
    pipeline.traverseTopologically(new DependentTransformsVisitor(translator, ctxt));

    assertEquals(1, ctxt.getPCollectionConsumptionMap().get(result.get(passOutTag)).intValue());
    assertEquals(
        1, ctxt.getPCollectionConsumptionMap().get(result.get(wordCountOutTag)).intValue());
    assertEquals(
        0, ctxt.getPCollectionConsumptionMap().get(result.get(lettersCountOutTag)).intValue());
  }
}
