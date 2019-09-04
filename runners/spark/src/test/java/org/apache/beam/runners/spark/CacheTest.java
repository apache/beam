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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.apache.beam.runners.spark.translation.Dataset;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

/** Tests of {@link Dataset#cache(String, Coder)}} scenarios. */
public class CacheTest {

  /**
   * Test checks how the cache candidates map is populated by the runner when evaluating the
   * pipeline.
   */
  @Test
  public void cacheCandidatesUpdaterTest() {
    SparkPipelineOptions options = createOptions();
    Pipeline pipeline = Pipeline.create(options);
    PCollection<String> pCollection = pipeline.apply(Create.of("foo", "bar"));

    // First use of pCollection.
    pCollection.apply(Count.globally());
    // Second use of pCollection.
    PCollectionView<List<String>> view = pCollection.apply(View.asList());

    // Internally View.asList() creates a PCollection that underlies the PCollectionView, that
    // PCollection should not be cached as the SparkRunner does not access that PCollection to
    // access the PCollectionView.
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

    JavaSparkContext jsc = SparkContextFactory.getSparkContext(options);
    EvaluationContext ctxt = new EvaluationContext(jsc, pipeline, options);
    SparkRunner.CacheVisitor cacheVisitor =
        new SparkRunner.CacheVisitor(new TransformTranslator.Translator(), ctxt);
    pipeline.traverseTopologically(cacheVisitor);
    assertEquals(2L, (long) ctxt.getCacheCandidates().get(pCollection));
    assertEquals(1L, ctxt.getCacheCandidates().values().stream().filter(l -> l > 1).count());
  }

  @Test
  public void shouldCacheTest() {
    SparkPipelineOptions options = createOptions();
    options.setCacheDisabled(true);
    Pipeline pipeline = Pipeline.create(options);

    Values<String> valuesTransform = Create.of("foo", "bar");
    PCollection pCollection = mock(PCollection.class);

    JavaSparkContext jsc = SparkContextFactory.getSparkContext(options);
    EvaluationContext ctxt = new EvaluationContext(jsc, pipeline, options);
    ctxt.getCacheCandidates().put(pCollection, 2L);

    assertFalse(ctxt.shouldCache(valuesTransform, pCollection));

    options.setCacheDisabled(false);
    assertTrue(ctxt.shouldCache(valuesTransform, pCollection));

    GroupByKey<String, String> gbkTransform = GroupByKey.create();
    assertFalse(ctxt.shouldCache(gbkTransform, pCollection));
  }

  private SparkPipelineOptions createOptions() {
    SparkPipelineOptions options =
        PipelineOptionsFactory.create().as(TestSparkPipelineOptions.class);
    options.setRunner(TestSparkRunner.class);
    return options;
  }
}
