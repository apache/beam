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
package org.apache.beam.runners.samza;

import java.util.UUID;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.runners.jobsubmission.JobInvoker;
import org.apache.beam.runners.jobsubmission.PortablePipelineJarCreator;
import org.apache.beam.runners.jobsubmission.PortablePipelineRunner;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SamzaJobInvoker extends JobInvoker {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaJobInvoker.class);

  public static SamzaJobInvoker create(
      SamzaJobServerDriver.SamzaServerConfiguration configuration) {
    return new SamzaJobInvoker();
  }

  private SamzaJobInvoker() {
    this("samza-runner-job-invoker-%d");
  }

  protected SamzaJobInvoker(String name) {
    super(name);
  }

  @Override
  protected JobInvocation invokeWithExecutor(
      RunnerApi.Pipeline pipeline,
      Struct options,
      String retrievalToken,
      ListeningExecutorService executorService) {
    LOG.trace("Parsing pipeline options");
    final SamzaPortablePipelineOptions samzaOptions =
        PipelineOptionsTranslation.fromProto(options).as(SamzaPortablePipelineOptions.class);

    final PortablePipelineRunner pipelineRunner;
    if (Strings.isNullOrEmpty(samzaOptions.getOutputExecutablePath())) {
      pipelineRunner = new SamzaPipelineRunner(samzaOptions);
    } else {
      /*
       * To support --output_executable_path where bundles the input pipeline along with all
       * artifacts, etc. required to run the pipeline into a jar that can be executed later.
       */
      pipelineRunner = new PortablePipelineJarCreator(SamzaPipelineRunner.class);
    }

    final String invocationId =
        String.format("%s_%s", samzaOptions.getJobName(), UUID.randomUUID().toString());
    final JobInfo jobInfo =
        JobInfo.create(invocationId, samzaOptions.getJobName(), retrievalToken, options);
    return new JobInvocation(jobInfo, executorService, pipeline, pipelineRunner);
  }
}
