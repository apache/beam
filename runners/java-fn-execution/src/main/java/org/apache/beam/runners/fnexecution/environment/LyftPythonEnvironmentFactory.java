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

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.LegacyArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.util.JsonFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EnvironmentFactory} that uses direct Python SDK harness process environments.
 *
 * <p>Assumes that all dependencies are present on the host machine and doesn't need to support
 * artifact retrieval. In the Python development environment, simply run the JVM within the Python
 * virtualenv.
 */
public class LyftPythonEnvironmentFactory implements EnvironmentFactory {

  private static final Logger LOG = LoggerFactory.getLogger(LyftPythonEnvironmentFactory.class);

  // the command that will be run via bash
  // default assumes execution within already activated virtualenv
  // env is added for debugging purposes, output is only visible when debug logging is enabled
  private static final String SDK_HARNESS_BASH_CMD =
      MoreObjects.firstNonNull(
          System.getenv("BEAM_PYTHON_WORKER_BASH_CMD"),
          System.getProperty(
              "lyft.pythonWorkerCmd", "env; python -m apache_beam.runners.worker.sdk_worker_main"));
  private static final int HARNESS_CONNECT_TIMEOUT_MINS = 5;

  private final JobInfo jobInfo;
  private final ProcessManager processManager;
  private final GrpcFnServer<FnApiControlClientPoolService> controlServiceServer;
  private final GrpcFnServer<GrpcLoggingService> loggingServiceServer;
  private final GrpcFnServer<LegacyArtifactRetrievalService> retrievalServiceServer;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;
  private final IdGenerator idGenerator;
  private final ControlClientPool.Source clientSource;

  private LyftPythonEnvironmentFactory(
      JobInfo jobInfo,
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<LegacyArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientPool.Source clientSource,
      IdGenerator idGenerator) {
    this.jobInfo = jobInfo;
    this.processManager = ProcessManager.create();
    this.controlServiceServer = controlServiceServer;
    this.loggingServiceServer = loggingServiceServer;
    this.retrievalServiceServer = retrievalServiceServer;
    this.provisioningServiceServer = provisioningServiceServer;
    this.idGenerator = idGenerator;
    this.clientSource = clientSource;
  }

  /** Creates a new, active {@link RemoteEnvironment} backed by a forked process. */
  @Override
  public RemoteEnvironment createEnvironment(RunnerApi.Environment environment) throws Exception {
    String workerId = idGenerator.getId();

    String pipelineOptionsJson = JsonFormat.printer().print(jobInfo.pipelineOptions());
    // https://issues.apache.org/jira/browse/BEAM-5509
    // pipelineOptionsJson =
    //    pipelineOptionsJson.replace(
    //        "beam:option:parallelism:v1", "beam:option:INVALIDparallelism:v1");
    HashMap<String, String> env = new HashMap<>();
    env.put("WORKER_ID", workerId);
    env.put("PIPELINE_OPTIONS", pipelineOptionsJson);
    env.put(
        "LOGGING_API_SERVICE_DESCRIPTOR",
        loggingServiceServer.getApiServiceDescriptor().toString());
    env.put(
        "CONTROL_API_SERVICE_DESCRIPTOR",
        controlServiceServer.getApiServiceDescriptor().toString());
    // env.put("SEMI_PERSISTENT_DIRECTORY", "/tmp");

    String executable = "bash";
    List<String> args = ImmutableList.of("-c", SDK_HARNESS_BASH_CMD);

    LOG.info("Creating Process with ID {}", workerId);
    // Wrap the blocking call to clientSource.get in case an exception is thrown.
    InstructionRequestHandler instructionHandler = null;
    try {
      final ProcessManager.RunningProcess runningProcess =
          processManager.startProcess(workerId, executable, args, env);
      // Wait for the SDK harness to connect to the gRPC server.
      long timeoutMillis =
          System.currentTimeMillis() + Duration.ofMinutes(HARNESS_CONNECT_TIMEOUT_MINS).toMillis();
      while (instructionHandler == null && System.currentTimeMillis() < timeoutMillis) {
        runningProcess.isAliveOrThrow();
        try {
          instructionHandler = clientSource.take(workerId, Duration.ofSeconds(30));
        } catch (TimeoutException timeoutEx) {
          LOG.info(
              "Still waiting for connection from command '{}' for worker id {}",
              SDK_HARNESS_BASH_CMD,
              workerId);
        } catch (InterruptedException interruptEx) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(interruptEx);
        }
      }

      if (instructionHandler == null) {
        String msg =
            String.format(
                "Timeout of %d minutes waiting for worker '%s' with command '%s' to connect to the service endpoint.",
                HARNESS_CONNECT_TIMEOUT_MINS, workerId, SDK_HARNESS_BASH_CMD);
        throw new TimeoutException(msg);
      }

    } catch (Exception e) {
      try {
        processManager.stopProcess(workerId);
      } catch (Exception processKillException) {
        e.addSuppressed(processKillException);
      }
      throw e;
    }

    return ProcessEnvironment.create(processManager, environment, workerId, instructionHandler);
  }

  /** Provider of ProcessEnvironmentFactory. */
  public static class Provider implements EnvironmentFactory.Provider {
    private final JobInfo jobInfo;

    public Provider(JobInfo jobInfo) {
      this.jobInfo = jobInfo;
    }

    @Override
    public EnvironmentFactory createEnvironmentFactory(
        GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
        GrpcFnServer<GrpcLoggingService> loggingServiceServer,
        GrpcFnServer<LegacyArtifactRetrievalService> retrievalServiceServer,
        GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
        ControlClientPool clientPool,
        IdGenerator idGenerator) {
      return new LyftPythonEnvironmentFactory(
          jobInfo,
          controlServiceServer,
          loggingServiceServer,
          retrievalServiceServer,
          provisioningServiceServer,
          clientPool.getSource(),
          idGenerator);
    }
  }
}
