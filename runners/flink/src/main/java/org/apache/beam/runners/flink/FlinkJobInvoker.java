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
package org.apache.beam.runners.flink;

import static org.apache.beam.runners.core.construction.PipelineResources.detectClassPathResourcesToStage;

import com.google.common.util.concurrent.ListeningExecutorService;
import java.io.IOException;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.apache.beam.vendor.protobuf.v3.com.google.protobuf.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Job Invoker for the {@link FlinkRunner}. */
public class FlinkJobInvoker implements JobInvoker {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobInvoker.class);

  public static FlinkJobInvoker create(
      ListeningExecutorService executorService, String flinkMasterUrl) {
    return new FlinkJobInvoker(executorService, flinkMasterUrl);
  }

  private final ListeningExecutorService executorService;
  private final String flinkMasterUrl;

  private FlinkJobInvoker(ListeningExecutorService executorService, String flinkMasterUrl) {
    this.executorService = executorService;
    this.flinkMasterUrl = flinkMasterUrl;
  }

  @Override
  public JobInvocation invoke(
      RunnerApi.Pipeline pipeline, Struct options, @Nullable String retrievalToken)
      throws IOException {
    // TODO: How to make Java/Python agree on names of keys and their values?
    LOG.trace("Parsing pipeline options");
    FlinkPipelineOptions flinkOptions =
        PipelineOptionsTranslation.fromProto(options).as(FlinkPipelineOptions.class);

    String invocationId =
        String.format("%s_%s", flinkOptions.getJobName(), UUID.randomUUID().toString());
    LOG.info("Invoking job {}", invocationId);

    flinkOptions.setFlinkMaster(flinkMasterUrl);

    flinkOptions.setRunner(null);

    return FlinkJobInvocation.create(
        invocationId,
        retrievalToken,
        executorService,
        pipeline,
        flinkOptions,
        detectClassPathResourcesToStage(FlinkJobInvoker.class.getClassLoader()));
  }
}
