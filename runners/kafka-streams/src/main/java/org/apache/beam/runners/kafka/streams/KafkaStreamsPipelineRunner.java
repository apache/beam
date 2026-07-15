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

import java.util.Properties;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.jobsubmission.PortablePipelineResult;
import org.apache.beam.runners.jobsubmission.PortablePipelineRunner;
import org.apache.beam.runners.kafka.streams.translation.KafkaStreamsPipelineTranslator;
import org.apache.beam.runners.kafka.streams.translation.KafkaStreamsTranslationContext;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes a portable pipeline by translating it to a Kafka Streams {@link Topology}. */
public class KafkaStreamsPipelineRunner implements PortablePipelineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsPipelineRunner.class);

  private final KafkaStreamsPipelineOptions pipelineOptions;

  public KafkaStreamsPipelineRunner(KafkaStreamsPipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public PortablePipelineResult run(RunnerApi.Pipeline pipeline, JobInfo jobInfo) {
    // Surface a clear error if a required option (e.g. applicationId) is missing instead of
    // letting Properties.put fail with a raw NullPointerException further down.
    PipelineOptionsValidator.validate(KafkaStreamsPipelineOptions.class, pipelineOptions);

    KafkaStreamsPipelineTranslator translator = new KafkaStreamsPipelineTranslator();
    KafkaStreamsTranslationContext context =
        translator.createTranslationContext(jobInfo, pipelineOptions);
    RunnerApi.Pipeline prepared = translator.prepareForTranslation(pipeline);
    translator.translate(context, prepared);

    Topology topology = context.getTopology();
    LOG.info(
        "Translated pipeline {} into Kafka Streams topology:\n{}",
        jobInfo.jobId(),
        topology.describe());

    KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig(jobInfo));
    kafkaStreams.start();
    return new KafkaStreamsPortablePipelineResult(
        kafkaStreams, context.getMetricsContainerStepMap());
  }

  private Properties streamsConfig(JobInfo jobInfo) {
    Properties props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, pipelineOptions.getBootstrapServers());
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, pipelineOptions.getApplicationId());
    props.put(StreamsConfig.STATE_DIR_CONFIG, pipelineOptions.getStateDir());
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    props.put(StreamsConfig.CLIENT_ID_CONFIG, jobInfo.jobId());
    return props;
  }
}
