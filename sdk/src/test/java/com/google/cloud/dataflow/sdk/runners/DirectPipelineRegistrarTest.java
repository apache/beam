/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.runners;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.options.DirectPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsRegistrar;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ServiceLoader;

/** Tests for {@link DirectPipelineRegistrar}. */
@RunWith(JUnit4.class)
public class DirectPipelineRegistrarTest {
  @Test
  public void testCorrectOptionsAreReturned() {
    assertEquals(ImmutableList.of(DirectPipelineOptions.class),
        new DirectPipelineRegistrar.Options().getPipelineOptions());
  }

  @Test
  public void testCorrectRunnersAreReturned() {
    assertEquals(ImmutableList.of(DirectPipelineRunner.class),
        new DirectPipelineRegistrar.Runner().getPipelineRunners());
  }

  @Test
  public void testServiceLoaderForOptions() {
    for (PipelineOptionsRegistrar registrar :
        Lists.newArrayList(ServiceLoader.load(PipelineOptionsRegistrar.class).iterator())) {
      if (registrar instanceof DirectPipelineRegistrar.Options) {
        return;
      }
    }
    fail("Expected to find " + DirectPipelineRegistrar.Options.class);
  }

  @Test
  public void testServiceLoaderForRunner() {
    for (PipelineRunnerRegistrar registrar :
        Lists.newArrayList(ServiceLoader.load(PipelineRunnerRegistrar.class).iterator())) {
      if (registrar instanceof DirectPipelineRegistrar.Runner) {
        return;
      }
    }
    fail("Expected to find " + DirectPipelineRegistrar.Runner.class);
  }
}
