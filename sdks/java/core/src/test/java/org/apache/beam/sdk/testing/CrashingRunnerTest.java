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
package org.apache.beam.sdk.testing;

import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CrashingRunner}. */
@RunWith(JUnit4.class)
public class CrashingRunnerTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void fromOptionsCreatesInstance() {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(CrashingRunner.class);
    PipelineRunner<? extends PipelineResult> runner = PipelineRunner.fromOptions(opts);

    assertTrue("Should have created a CrashingRunner", runner instanceof CrashingRunner);
  }

  @Test
  public void applySucceeds() {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(CrashingRunner.class);

    Pipeline p = Pipeline.create(opts);
    p.apply(Create.of(1, 2, 3));
  }

  @Test
  public void runThrows() {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(CrashingRunner.class);

    Pipeline p = Pipeline.create(opts);
    p.apply(Create.of(1, 2, 3));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Cannot call #run");
    thrown.expectMessage(TestPipeline.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS);

    p.run();
  }
}
