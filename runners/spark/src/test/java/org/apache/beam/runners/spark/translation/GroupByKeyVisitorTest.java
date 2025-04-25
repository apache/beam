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
package org.apache.beam.runners.spark.translation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

import org.apache.beam.runners.spark.SparkContextRule;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests of {@link GroupByKeyVisitor}}. */
public class GroupByKeyVisitorTest {

  @ClassRule public static SparkContextRule contextRule = new SparkContextRule();

  @Test
  public void testTraverseShouldPopulateCandidatesIntoEvaluationContext() {
    SparkPipelineOptions options = contextRule.createPipelineOptions();
    Pipeline pipeline = Pipeline.create(options);
    PCollection<KV<Integer, String>> pCollection =
        pipeline.apply(Create.of(KV.of(3, "foo"), KV.of(3, "bar")));

    pCollection.apply("CandidateGBK_1", Reshuffle.viaRandomKey());
    pCollection.apply("CandidateGBK_2", GroupByKey.create());

    final TupleTag<String> t1 = new TupleTag<>();
    final TupleTag<String> t2 = new TupleTag<>();

    KeyedPCollectionTuple.of(t1, pCollection)
        .and(t2, pCollection)
        .apply("GBK_inside_CoGBK_ignored", CoGroupByKey.create());

    EvaluationContext ctxt =
        new EvaluationContext(contextRule.getSparkContext(), pipeline, options);
    GroupByKeyVisitor visitor = new GroupByKeyVisitor(new TransformTranslator.Translator(), ctxt);
    pipeline.traverseTopologically(visitor);

    assertEquals(3, visitor.getVisitedGroupByKeyTransformsCount());
    assertEquals(2, ctxt.getCandidatesForGroupByKeyAndWindowTranslation().size());
    assertThat(
        ctxt.getCandidatesForGroupByKeyAndWindowTranslation().values(),
        containsInAnyOrder("CandidateGBK_1/Reshuffle/GroupByKey", "CandidateGBK_2"));
  }
}
