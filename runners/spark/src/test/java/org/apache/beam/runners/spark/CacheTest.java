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

import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Rule;
import org.junit.Test;

/**
 * This test checks how the cache candidates map is populated by the runner when evaluating the
 * pipeline.
 */
public class CacheTest {

  @Rule
  public final transient PipelineRule pipelineRule = PipelineRule.batch();

  @Test
  public void cacheCandidatesUpdaterTest() throws Exception {
    Pipeline pipeline = pipelineRule.createPipeline();
    PCollection<String> pCollection = pipeline.apply(Create.of("foo", "bar"));
    // first read
    pCollection.apply(Count.<String>globally());
    // second read
    // as we access the same PCollection two times, the Spark runner does optimization and so
    // will cache the RDD representing this PCollection
    pCollection.apply(Count.<String>globally());

    JavaSparkContext jsc = SparkContextFactory.getSparkContext(pipelineRule.getOptions());
    EvaluationContext ctxt = new EvaluationContext(jsc, pipeline);
    SparkRunner.CacheVisitor cacheVisitor =
        new SparkRunner.CacheVisitor(new TransformTranslator.Translator(), ctxt);
    pipeline.traverseTopologically(cacheVisitor);
    assertEquals(2L, (long) ctxt.getCacheCandidates().get(pCollection));
  }

}
