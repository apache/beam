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
package org.apache.beam.runners.jstorm;

import java.io.IOException;
import java.io.Serializable;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link JStormRunner}.
 */
@RunWith(JUnit4.class)
public class JStormRunnerTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(JStormRunnerTest.class);

  private Pipeline createPipeline() {
    PipelineOptions options = PipelineOptionsFactory.as(JStormPipelineOptions.class);
    options.setRunner(TestJStormRunner.class);
    return Pipeline.create(options);
  }

  private class TestDoFn extends DoFn<Integer, Integer> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(context.element());
    }
  }

  @Test
  public void testCreatePipeline() {
    Pipeline pipeline = createPipeline();
    pipeline.apply(Create.of(1))
        .apply(ParDo.of(new TestDoFn()));
    PipelineResult result = pipeline.run();
    try {
      result.cancel();
    } catch (IOException e) {
      LOG.error("Failed to cancel pipeline", e);
    }
  }
}
