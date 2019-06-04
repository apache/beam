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
package org.apache.beam.runners.flink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

/** Tests for {@link PipelineTranslationModeOptimizer}. */
public class PipelineTranslationModeOptimizerTest {

  @Test
  public void testUnboundedCollectionProducingTransform() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(FlinkRunner.class);
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(GenerateSequence.from(0));

    assertThat(PipelineTranslationModeOptimizer.hasUnboundedOutput(pipeline), is(true));
  }

  @Test
  public void testBoundedCollectionProducingTransform() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(FlinkRunner.class);
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(GenerateSequence.from(0).to(10));

    assertThat(PipelineTranslationModeOptimizer.hasUnboundedOutput(pipeline), is(false));
  }
}
