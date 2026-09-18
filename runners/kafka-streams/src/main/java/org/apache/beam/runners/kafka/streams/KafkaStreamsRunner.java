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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.portability.PortableRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} that submits portable jobs to an in-process or external Beam job service
 * backed by the Kafka Streams translation path.
 *
 * <p><b>This runner is experimental.</b> It executes a subset of the Beam model correctly — the
 * parts it supports are covered by Beam's {@code @ValidatesRunner} suite — but several capabilities
 * that are core to the model are not implemented yet, among them side inputs, stateful {@code
 * ParDo} and user timers, merging windows, custom {@code WindowFn}s and splittable {@code DoFn}.
 * Its behaviour and its pipeline options may change. See the <a
 * href="https://beam.apache.org/documentation/runners/kafkastreams/">runner documentation</a> for
 * what is and is not supported, and <a
 * href="https://github.com/apache/beam/issues/18479">#18479</a> for the work that remains.
 */
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
    prepareForTranslation(pipeline, pipelineOptions);
    @Nullable KafkaStreamsJobServerDriver jobServerDriver = null;
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
        KafkaStreamsJobServerDriver driverForStop = jobServerDriver;
        return new KafkaStreamsPipelineResult(result, driverForStop::stop);
      }
      return result;
    } catch (Exception e) {
      if (jobServerDriver != null) {
        jobServerDriver.stop();
      }
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Settles the options the runner needs and rewrites the pipeline into what it can translate.
   *
   * <p>The runner does not translate splittable DoFns, and a {@link org.apache.beam.sdk.io.Read}
   * expands into one by default, so a pipeline that merely reads would otherwise fail to translate.
   * Beam keeps the primitive read for exactly this case, behind an experiment that {@link
   * #assignPortableDefaults} sets, so a pipeline does not have to ask for it and the proto that
   * reaches the job server already holds primitive reads.
   */
  @VisibleForTesting
  static void prepareForTranslation(
      Pipeline pipeline, KafkaStreamsPipelineOptions pipelineOptions) {
    assignPortableDefaults(pipelineOptions);
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReadsIfNecessary(pipeline);
  }

  private static void assignPortableDefaults(KafkaStreamsPipelineOptions pipelineOptions) {
    if (Strings.isNullOrEmpty(pipelineOptions.getDefaultEnvironmentType())) {
      pipelineOptions.setDefaultEnvironmentType(Environments.ENVIRONMENT_LOOPBACK);
    }
    ExperimentalOptions experimentalOptions = pipelineOptions.as(ExperimentalOptions.class);
    @Nullable List<String> existingExperiments = experimentalOptions.getExperiments();
    List<String> experiments =
        existingExperiments == null ? new ArrayList<>() : new ArrayList<>(existingExperiments);
    boolean changed = false;
    if (!experiments.contains("beam_fn_api")) {
      experiments.add("beam_fn_api");
      changed = true;
    }
    // Splittable DoFns are not translated, so the Read that expands into one has to stay the
    // primitive it used to be. This is the experiment Beam looks for when deciding that.
    if (!experiments.contains("use_deprecated_read")) {
      experiments.add("use_deprecated_read");
      changed = true;
    }
    if (changed) {
      experimentalOptions.setExperiments(experiments);
    }
  }

  @Override
  public String toString() {
    return "KafkaStreamsRunner#" + hashCode();
  }
}
