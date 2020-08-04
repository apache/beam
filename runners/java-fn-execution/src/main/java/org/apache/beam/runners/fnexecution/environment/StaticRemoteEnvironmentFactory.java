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
package org.apache.beam.runners.fnexecution.environment;

import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.IdGenerator;

/**
 * An {@link EnvironmentFactory} that creates StaticRemoteEnvironment used by a runner harness that
 * would like to use an existing InstructionRequestHandler.
 */
public class StaticRemoteEnvironmentFactory implements EnvironmentFactory {
  public static StaticRemoteEnvironmentFactory forService(
      InstructionRequestHandler instructionRequestHandler) {
    return new StaticRemoteEnvironmentFactory(instructionRequestHandler);
  }

  private final InstructionRequestHandler instructionRequestHandler;

  private StaticRemoteEnvironmentFactory(InstructionRequestHandler instructionRequestHandler) {
    this.instructionRequestHandler = instructionRequestHandler;
  }

  @Override
  public RemoteEnvironment createEnvironment(Environment environment, String workerId) {
    return StaticRemoteEnvironment.create(environment, this.instructionRequestHandler);
  }

  /** Provider for StaticRemoteEnvironmentFactory. */
  public static class Provider implements EnvironmentFactory.Provider {
    private final InstructionRequestHandler instructionRequestHandler;

    public Provider(InstructionRequestHandler instructionRequestHandler) {
      this.instructionRequestHandler = instructionRequestHandler;
    }

    @Override
    public EnvironmentFactory createEnvironmentFactory(
        GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
        GrpcFnServer<GrpcLoggingService> loggingServiceServer,
        GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
        GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
        ControlClientPool clientPool,
        IdGenerator idGenerator) {
      return StaticRemoteEnvironmentFactory.forService(this.instructionRequestHandler);
    }
  }
}
