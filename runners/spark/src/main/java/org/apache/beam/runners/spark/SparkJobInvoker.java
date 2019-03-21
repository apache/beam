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
package org.apache.beam.runners.spark;

import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates a job invocation to manage the Spark runner's execution of a portable pipeline. */
public class SparkJobInvoker extends JobInvoker {

  private static final Logger LOG = LoggerFactory.getLogger(SparkJobInvoker.class);

  public static SparkJobInvoker create() {
    return new SparkJobInvoker();
  }

  private SparkJobInvoker() {
    super("spark-runner-job-invoker");
  }

  @Override
  protected JobInvocation invokeWithExecutor(
      Pipeline pipeline,
      Struct options,
      @Nullable String retrievalToken,
      ListeningExecutorService executorService) {
    LOG.trace("Parsing pipeline options");
    SparkPipelineOptions sparkOptions =
        PipelineOptionsTranslation.fromProto(options).as(SparkPipelineOptions.class);

    String invocationId =
        String.format("%s_%s", sparkOptions.getJobName(), UUID.randomUUID().toString());
    LOG.info("Invoking job {}", invocationId);

    return createJobInvocation(
        invocationId, retrievalToken, executorService, pipeline, sparkOptions);
  }

  static JobInvocation createJobInvocation(
      String invocationId,
      String retrievalToken,
      ListeningExecutorService executorService,
      Pipeline pipeline,
      SparkPipelineOptions sparkOptions) {
    JobInfo jobInfo =
        JobInfo.create(
            invocationId,
            sparkOptions.getJobName(),
            retrievalToken,
            PipelineOptionsTranslation.toProto(sparkOptions));
    SparkPipelineRunner pipelineRunner = new SparkPipelineRunner(sparkOptions);
    return new JobInvocation(jobInfo, executorService, pipeline, pipelineRunner);
  }
}
