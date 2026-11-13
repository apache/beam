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
package org.apache.beam.runners.spark.structuredstreaming.translation;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.runners.spark.SparkCommonPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.PipelineTranslatorBatch;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PipelineTranslator}. */
@RunWith(JUnit4.class)
public class PipelineTranslatorTest {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Test
  public void leavesFollowTranslationOrder() {
    SparkCommonPipelineOptions options =
        SESSION.createPipelineOptions().as(SparkCommonPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    PCollection<byte[]> input = pipeline.apply(Impulse.create());
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      expected.add(
          input
              .apply("Leaf" + i, MapElements.into(TypeDescriptors.integers()).via(x -> x.length))
              .getName());
    }

    EvaluationContext context =
        new PipelineTranslatorBatch().translate(pipeline, SESSION.getSession(), options);

    assertEquals(
        expected,
        context.leaves().stream()
            .map(EvaluationContext.NamedDataset::name)
            .collect(Collectors.toList()));
  }
}
