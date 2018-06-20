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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.Struct;
import java.io.IOException;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job Invoker for the {@link FlinkRunner}.
 */
public class FlinkJobInvoker implements JobInvoker {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobInvoker.class);

  public static FlinkJobInvoker create(ListeningExecutorService executorService) {
    return new FlinkJobInvoker(executorService);
  }

  private final ListeningExecutorService executorService;

  private FlinkJobInvoker(ListeningExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  public JobInvocation invoke(
      RunnerApi.Pipeline pipeline, Struct options, @Nullable String retrievalToken)
      throws IOException {
    // TODO: How to make Java/Python agree on names of keys and their values?
    LOG.trace("Parsing pipeline options");
    FlinkPipelineOptions flinkOptions = PipelineOptionsTranslation.fromProto(options)
        .as(FlinkPipelineOptions.class);

    String invocationId = String.format(
        "%s_%s", flinkOptions.getJobName(), UUID.randomUUID().toString());
    LOG.info("Invoking job {}", invocationId);

    // Set Flink Master to [auto] if no option was specified.
    if (flinkOptions.getFlinkMaster() == null) {
      flinkOptions.setFlinkMaster("[auto]");
    }

    flinkOptions.setRunner(null);

    return FlinkJobInvocation.create(
        invocationId,
        retrievalToken,
        executorService,
        pipeline,
        flinkOptions);
  }
}
