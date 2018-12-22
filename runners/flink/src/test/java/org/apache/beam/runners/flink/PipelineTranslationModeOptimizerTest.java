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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

/**
 * Traverses the Pipeline to determine the translation mode (i.e. streaming or batch) for this
 * pipeline.
 */
public class PipelineTranslationModeOptimizerTest {

  @Test
  public void testTranslationModeOverrideWithUnboundedSources() {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    options.setStreaming(false);

    FlinkPipelineExecutionEnvironment flinkEnv = new FlinkPipelineExecutionEnvironment(options);
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(GenerateSequence.from(0));
    flinkEnv.translate(pipeline);

    assertThat(options.isStreaming(), is(true));
  }

  @Test
  public void testTranslationModeNoOverrideWithoutUnboundedSources() {
    boolean[] testArgs = new boolean[] {true, false};
    for (boolean streaming : testArgs) {
      FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
      options.setRunner(FlinkRunner.class);
      options.setStreaming(streaming);

      FlinkPipelineExecutionEnvironment flinkEnv = new FlinkPipelineExecutionEnvironment(options);
      Pipeline pipeline = Pipeline.create(options);
      flinkEnv.translate(pipeline);

      assertThat(options.isStreaming(), is(streaming));
    }
  }
}
