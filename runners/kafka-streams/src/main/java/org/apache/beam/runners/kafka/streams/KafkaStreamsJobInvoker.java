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

import java.util.UUID;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.runners.jobsubmission.JobInvoker;
import org.apache.beam.runners.jobsubmission.PortablePipelineRunner;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Job invoker for the Kafka Streams portable runner. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class KafkaStreamsJobInvoker extends JobInvoker {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsJobInvoker.class);

  public static KafkaStreamsJobInvoker create(
      KafkaStreamsJobServerDriver.KafkaStreamsServerConfiguration serverConfig) {
    return new KafkaStreamsJobInvoker(serverConfig);
  }

  private final KafkaStreamsJobServerDriver.KafkaStreamsServerConfiguration serverConfig;

  protected KafkaStreamsJobInvoker(
      KafkaStreamsJobServerDriver.KafkaStreamsServerConfiguration serverConfig) {
    super("kafka-streams-runner-job-invoker-%d");
    this.serverConfig = serverConfig;
  }

  @Override
  protected JobInvocation invokeWithExecutor(
      RunnerApi.Pipeline pipeline,
      Struct options,
      @Nullable String retrievalToken,
      ListeningExecutorService executorService) {

    LOG.trace(
        "Parsing pipeline options (job server {}:{})",
        serverConfig.getHost(),
        serverConfig.getPort());
    KafkaStreamsPipelineOptions kafkaStreamsOptions =
        PipelineOptionsTranslation.fromProto(options).as(KafkaStreamsPipelineOptions.class);

    String invocationId =
        String.format("%s_%s", kafkaStreamsOptions.getJobName(), UUID.randomUUID().toString());

    PortablePipelineRunner pipelineRunner = new KafkaStreamsPipelineRunner(kafkaStreamsOptions);
    kafkaStreamsOptions.setRunner(null);

    LOG.info("Invoking job {} with pipeline runner {}", invocationId, pipelineRunner);
    return createJobInvocation(
        invocationId,
        retrievalToken,
        executorService,
        pipeline,
        kafkaStreamsOptions,
        pipelineRunner);
  }

  protected JobInvocation createJobInvocation(
      String invocationId,
      String retrievalToken,
      ListeningExecutorService executorService,
      RunnerApi.Pipeline pipeline,
      KafkaStreamsPipelineOptions kafkaStreamsOptions,
      PortablePipelineRunner pipelineRunner) {
    JobInfo jobInfo =
        JobInfo.create(
            invocationId,
            kafkaStreamsOptions.getJobName(),
            retrievalToken,
            PipelineOptionsTranslation.toProto(kafkaStreamsOptions));
    return new JobInvocation(jobInfo, executorService, pipeline, pipelineRunner);
  }
}
