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
package org.apache.beam.runners.samza.util;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;

public class DoFnUtilsTest implements Serializable {
  private final Pipeline pipeline = Pipeline.create();

  @Test
  public void testExecutableStageWithoutOutput() {
    pipeline.apply(Create.of(KV.of(1L, "1")));

    assertEquals("[Create.Values-]", DoFnUtils.toStepName(getOnlyExecutableStage(pipeline)));
  }

  @Test
  public void testExecutableStageWithCustomizedName() {
    pipeline.apply("MyCreateOf", Create.of(KV.of(1L, "1")));
    assertEquals("[MyCreateOf-]", DoFnUtils.toStepName(getOnlyExecutableStage(pipeline)));
  }

  @Test
  public void testExecutableStageWithOutput() {
    pipeline
        .apply("MyCreateOf", Create.of(KV.of(1L, "1")))
        .apply("MyFilterBy", Filter.by(Objects::nonNull))
        .apply(GroupByKey.create());

    assertEquals("[MyCreateOf-MyFilterBy]", DoFnUtils.toStepName(getOnlyExecutableStage(pipeline)));
  }

  @Test
  public void testExecutableStageWithPDone() {
    pipeline
        .apply("MyCreateOf", Create.of("1"))
        .apply(
            "PDoneTransform",
            new PTransform<PCollection<String>, PDone>() {
              @Override
              public PDone expand(PCollection<String> input) {
                return PDone.in(pipeline);
              }
            });

    assertEquals("[MyCreateOf-]", DoFnUtils.toStepName(getOnlyExecutableStage(pipeline)));
  }

  private static ExecutableStage getOnlyExecutableStage(Pipeline p) {
    return Iterables.getOnlyElement(
        GreedyPipelineFuser.fuse(PipelineTranslation.toProto(p)).getFusedStages());
  }
}
