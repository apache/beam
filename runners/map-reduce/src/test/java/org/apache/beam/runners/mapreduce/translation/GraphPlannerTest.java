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
package org.apache.beam.runners.mapreduce.translation;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import org.apache.beam.runners.mapreduce.MapReducePipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link GraphPlanner}.
 */
@RunWith(JUnit4.class)
public class GraphPlannerTest {

  @Test
  public void testCombine() throws Exception {
    MapReducePipelineOptions options = PipelineOptionsFactory.as(MapReducePipelineOptions.class);
    options.setRunner(CrashingRunner.class);
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String, Integer>> input = p
        .apply(Create.empty(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
        .apply(Sum.<String>integersPerKey());

    TranslationContext context = new TranslationContext(options);
    GraphConverter graphConverter = new GraphConverter(context);
    p.traverseTopologically(graphConverter);

    GraphPlanner planner = new GraphPlanner();
    Graphs.FusedGraph fusedGraph = planner.plan(context.getInitGraph());

    assertEquals(1, Iterables.size(fusedGraph.getFusedSteps()));
    assertEquals(3, Iterables.getOnlyElement(fusedGraph.getFusedSteps()).getSteps().size());
  }
}
