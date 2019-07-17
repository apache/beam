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

import java.util.EnumSet;
import java.util.Set;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

/**
 * A convenient class to provide fast lookup of enabled experiments in the worker code.
 *
 * <p>To add a new experiment, update the {@link Experiment} enum.
 *
 * <p>To determine if an experiment is enabled, instantiate an {@link ExperimentContext} with the
 * {@link PipelineOptions} and call {@link #isEnabled} to test if it is enabled.
 */
public class ExperimentContext {

  /** Enumeration of all known experiments. */
  public enum Experiment {
    /**
     * Use the Conscrypt OpenSSL Java Security Provider. This may improve the performance of SSL
     * operations for some IO connectors.
     */
    EnableConscryptSecurityProvider("enable_conscrypt_security_provider"),
    IntertransformIO("intertransform_io"), // Intertransform metrics for Shuffle IO (insights)
    SideInputIOMetrics("sideinput_io_metrics"); // Intertransform metrics for Side Input IO

    private final String name;

    Experiment(String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  private final EnumSet<Experiment> enabledExperiments = EnumSet.noneOf(Experiment.class);

  /**
   * Create an {@link ExperimentContext} from the given {@link PipelineOptions}.
   *
   * <p>NOTE: This will parse the experiment strings in the given options, so it should not be
   * re-parsed frequently.
   */
  public static ExperimentContext parseFrom(PipelineOptions options) {
    return new ExperimentContext(options.as(DataflowPipelineDebugOptions.class).getExperiments());
  }

  @VisibleForTesting
  static ExperimentContext parseFrom(Iterable<String> experimentNames) {
    return new ExperimentContext(experimentNames);
  }

  private ExperimentContext(Iterable<String> experimentNames) {
    if (experimentNames == null) {
      return;
    }
    Set<String> strings = Sets.newHashSet(experimentNames);
    for (Experiment experiment : Experiment.values()) {
      if (strings.contains(experiment.getName())) {
        enabledExperiments.add(experiment);
      }
    }
  }

  public boolean isEnabled(Experiment exp) {
    return enabledExperiments.contains(exp);
  }
}
