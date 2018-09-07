package org.apache.beam.runners.fnexecution.environment;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Meant to be replaced in Samza Portable Runner
 */
public class PassiveEnvironmentFactory implements EnvironmentFactory {
  public static final String SINGLE_WORKER_ID = "SINGLE_WORKER_ID";

  private static final Logger LOG = LoggerFactory.getLogger(PassiveEnvironmentFactory.class);

  private final GrpcFnServer<FnApiControlClientPoolService> controlServiceServer;
  private final GrpcFnServer<GrpcLoggingService> loggingServiceServer;
  private final GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;
  private final ControlClientPool.Source clientSource;

  public static PassiveEnvironmentFactory forServices(GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer, ControlClientPool.Source clientSource) {
    return new PassiveEnvironmentFactory(controlServiceServer, loggingServiceServer, retrievalServiceServer,
        provisioningServiceServer, clientSource);
  }

  private PassiveEnvironmentFactory(
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientPool.Source clientSource) {
    this.controlServiceServer = controlServiceServer;
    this.loggingServiceServer = loggingServiceServer;
    this.retrievalServiceServer = retrievalServiceServer;
    this.provisioningServiceServer = provisioningServiceServer;
    this.clientSource = clientSource;
  }

  @Override
  public RemoteEnvironment createEnvironment(RunnerApi.Environment environment) throws Exception {
    final String workerId = SINGLE_WORKER_ID;

    InstructionRequestHandler instructionHandler = null;
    while (instructionHandler == null) {
      try {
        instructionHandler = clientSource.take(workerId, Duration.ofSeconds(30));
      } catch (TimeoutException timeoutEx) {
        LOG.info("Still waiting for connection for worker id {}", workerId);
      } catch (InterruptedException interruptEx) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(interruptEx);
      }
    }

    return PassiveEnvironment.create(environment, instructionHandler);
  }
}
