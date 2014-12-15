/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PipelineOptions}. */
@RunWith(JUnit4.class)
public class PipelineOptionsTest {
  /** Interface used for testing that {@link PipelineOptions#as(Class)} functions. */
  public static interface TestOptions extends PipelineOptions {
  }

  @Test
  public void testDynamicAs() {
    TestOptions options = PipelineOptionsFactory.create().as(TestOptions.class);
    assertNotNull(options);
  }

  @Test
  public void testDefaultRunnerIsSet() {
    assertEquals(DirectPipelineRunner.class, PipelineOptionsFactory.create().getRunner());
  }
}
