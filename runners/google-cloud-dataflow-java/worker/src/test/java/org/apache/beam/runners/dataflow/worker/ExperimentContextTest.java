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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.ExperimentContext.Experiment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ExperimentContextTest}. */
@RunWith(JUnit4.class)
public class ExperimentContextTest {

  @Test
  public void testAllExperiments() {
    Set<String> experimentNames = new HashSet<>();
    ExperimentContext ec = ExperimentContext.parseFrom(experimentNames);
    // So far nothing is enabled.
    for (Experiment experiment : Experiment.values()) {
      assertFalse(ec.isEnabled(experiment));
    }

    // Now set all experiments.
    for (Experiment experiment : Experiment.values()) {
      experimentNames.add(experiment.getName());
    }
    ec = ExperimentContext.parseFrom(experimentNames);
    // They should all be enabled now.
    for (Experiment experiment : Experiment.values()) {
      assertTrue(ec.isEnabled(experiment));
    }
  }

  @Test
  public void testInitializeFromPipelineOptions() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(DataflowPipelineDebugOptions.class)
        .setExperiments(Lists.newArrayList(Experiment.IntertransformIO.getName()));
    ExperimentContext ec = ExperimentContext.parseFrom(options);
    assertTrue(ec.isEnabled(Experiment.IntertransformIO));
  }

  @Test
  public void testUnsetExperimentsInPipelineOptions() {
    PipelineOptions options = PipelineOptionsFactory.create();
    ExperimentContext ec = ExperimentContext.parseFrom(options);
    assertFalse(ec.isEnabled(Experiment.IntertransformIO));
  }
}
