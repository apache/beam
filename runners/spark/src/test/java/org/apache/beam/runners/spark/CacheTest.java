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
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.PCollection;
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
    // first read
    pCollection.apply(Count.globally());
    // second read
    // as we access the same PCollection two times, the Spark runner does optimization and so
    // will cache the RDD representing this PCollection
    pCollection.apply(Count.globally());

    JavaSparkContext jsc = SparkContextFactory.getSparkContext(options);
    EvaluationContext ctxt = new EvaluationContext(jsc, pipeline, options);
    SparkRunner.CacheVisitor cacheVisitor =
        new SparkRunner.CacheVisitor(new TransformTranslator.Translator(), ctxt);
    pipeline.traverseTopologically(cacheVisitor);
    assertEquals(2L, (long) ctxt.getCacheCandidates().get(pCollection));
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
