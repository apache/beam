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
package org.apache.beam.runners.prism;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ServiceLoader;
import org.apache.beam.runners.prism.PrismRegistrar.Options;
import org.apache.beam.runners.prism.PrismRegistrar.Runner;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrismRegistrar}. */
@RunWith(JUnit4.class)
public class PrismRegistrarTest {
  @Test
  public void testCorrectOptionsAreReturned() {
    assertEquals(
        ImmutableList.of(PrismPipelineOptions.class, TestPrismPipelineOptions.class),
        new Options().getPipelineOptions());
  }

  @Test
  public void testCorrectRunnersAreReturned() {
    assertEquals(
        ImmutableList.of(PrismRunner.class, TestPrismRunner.class),
        new Runner().getPipelineRunners());
  }

  @Test
  public void testServiceLoaderForOptions() {
    for (PipelineOptionsRegistrar registrar :
        Lists.newArrayList(ServiceLoader.load(PipelineOptionsRegistrar.class).iterator())) {
      if (registrar instanceof Options) {
        return;
      }
    }
    fail("Expected to find " + Options.class);
  }

  @Test
  public void testServiceLoaderForRunner() {
    for (PipelineRunnerRegistrar registrar :
        Lists.newArrayList(ServiceLoader.load(PipelineRunnerRegistrar.class).iterator())) {
      if (registrar instanceof Runner) {
        return;
      }
    }
    fail("Expected to find " + Runner.class);
  }
}
