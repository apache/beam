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
package org.apache.beam.runners.kafka.streams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.portability.PortableRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} that submits portable jobs to an in-process or external Beam job service
 * backed by the Kafka Streams translation path.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class KafkaStreamsRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsRunner.class);

  private final KafkaStreamsPipelineOptions pipelineOptions;

  public static KafkaStreamsRunner fromOptions(PipelineOptions options) {
    return new KafkaStreamsRunner(options.as(KafkaStreamsPipelineOptions.class));
  }

  protected KafkaStreamsRunner(KafkaStreamsPipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    assignPortableDefaults(pipelineOptions);
    KafkaStreamsJobServerDriver jobServerDriver = null;
    try {
      if (Strings.isNullOrEmpty(pipelineOptions.getJobEndpoint())) {
        LOG.info("No job endpoint configured; starting an embedded Kafka Streams job server.");
        KafkaStreamsJobServerDriver.KafkaStreamsServerConfiguration configuration =
            new KafkaStreamsJobServerDriver.KafkaStreamsServerConfiguration();
        configuration.setPort(0);
        jobServerDriver = KafkaStreamsJobServerDriver.fromConfig(configuration);
        pipelineOptions.setJobEndpoint(jobServerDriver.start());
      }
      PortableRunner portableRunner = PortableRunner.fromOptions(pipelineOptions);
      PipelineResult result = portableRunner.run(pipeline);
      if (jobServerDriver != null) {
        return new KafkaStreamsPipelineResult(result, jobServerDriver::stop);
      }
      return result;
    } catch (IOException e) {
      if (jobServerDriver != null) {
        jobServerDriver.stop();
      }
      throw new RuntimeException(e);
    }
  }

  private static void assignPortableDefaults(KafkaStreamsPipelineOptions pipelineOptions) {
    if (Strings.isNullOrEmpty(pipelineOptions.getDefaultEnvironmentType())) {
      pipelineOptions.setDefaultEnvironmentType(Environments.ENVIRONMENT_LOOPBACK);
    }
    ExperimentalOptions experimentalOptions = pipelineOptions.as(ExperimentalOptions.class);
    List<String> experiments =
        experimentalOptions.getExperiments() == null
            ? new ArrayList<>()
            : new ArrayList<>(experimentalOptions.getExperiments());
    if (!experiments.contains("beam_fn_api")) {
      experiments.add("beam_fn_api");
      experimentalOptions.setExperiments(experiments);
    }
  }

  @Override
  public String toString() {
    return "KafkaStreamsRunner#" + hashCode();
  }
}
