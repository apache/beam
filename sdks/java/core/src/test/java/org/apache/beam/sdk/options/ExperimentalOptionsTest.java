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
package org.apache.beam.sdk.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ExperimentalOptions}. */
@RunWith(JUnit4.class)
public class ExperimentalOptionsTest {
  @Test
  public void testExperimentIsSet() {
    ExperimentalOptions options =
        PipelineOptionsFactory.fromArgs("--experiments=experimentA,experimentB")
            .as(ExperimentalOptions.class);
    assertTrue(ExperimentalOptions.hasExperiment(options, "experimentA"));
    assertTrue(ExperimentalOptions.hasExperiment(options, "experimentB"));
    assertFalse(ExperimentalOptions.hasExperiment(options, "experimentC"));
  }

  @Test
  public void testExperimentGetValue() {
    ExperimentalOptions options =
        PipelineOptionsFactory.fromArgs(
                "--experiments=experimentA=0,state_cache_size=1,experimentB=0")
            .as(ExperimentalOptions.class);
    assertEquals(
        "1", ExperimentalOptions.getExperimentValue(options, ExperimentalOptions.STATE_CACHE_SIZE));
  }
}
