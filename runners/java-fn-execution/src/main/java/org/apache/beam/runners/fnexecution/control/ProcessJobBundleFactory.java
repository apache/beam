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

package org.apache.beam.runners.fnexecution.control;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.ProcessEnvironmentFactory;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link JobBundleFactoryBase} which uses a {@link ProcessEnvironmentFactory} to run the SDK
 * harness in an external process.
 */
public class ProcessJobBundleFactory extends JobBundleFactoryBase {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessJobBundleFactory.class);

  public static ProcessJobBundleFactory create(JobInfo jobInfo) throws Exception {
    return new ProcessJobBundleFactory(jobInfo);
  }

  protected ProcessJobBundleFactory(JobInfo jobInfo) throws Exception {
    super(jobInfo);
  }

  @VisibleForTesting
  ProcessJobBundleFactory(
      ProcessEnvironmentFactory envFactory,
      ServerFactory serverFactory,
      IdGenerator stageIdGenerator,
      GrpcFnServer<FnApiControlClientPoolService> controlServer,
      GrpcFnServer<GrpcLoggingService> loggingServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServer) {
    super(
        envFactory,
        serverFactory,
        stageIdGenerator,
        controlServer,
        loggingServer,
        retrievalServer,
        provisioningServer);
  }

  @Override
  protected EnvironmentFactory getEnvironmentFactory(
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientPool.Source clientSource,
      IdGenerator idGenerator) {
    return ProcessEnvironmentFactory.create(
        controlServiceServer,
        loggingServiceServer,
        retrievalServiceServer,
        provisioningServiceServer,
        clientSource,
        idGenerator);
  }
}
