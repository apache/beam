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
package org.apache.beam.runners.fnexecution;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.control.MapControlClientPool;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EmbeddedEnvironmentFactory;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/**
 * A {@link TestRule} which creates a {@link FnHarness} in a thread, services required for that
 * {@link FnHarness} to properly execute, and provides access to the associated client and harness
 * during test execution.
 */
public class EmbeddedSdkHarness extends ExternalResource implements TestRule {

  public static EmbeddedSdkHarness create() {
    return new EmbeddedSdkHarness();
  }

  private ExecutorService executor;
  private GrpcFnServer<GrpcLoggingService> loggingServer;
  private GrpcFnServer<GrpcDataService> dataServer;
  private GrpcFnServer<FnApiControlClientPoolService> controlServer;

  private SdkHarnessClient client;

  private EmbeddedSdkHarness() {}

  public SdkHarnessClient client() {
    return client;
  }

  public ApiServiceDescriptor dataEndpoint() {
    return dataServer.getApiServiceDescriptor();
  }

  @Override
  protected void before() throws Exception {
    InProcessServerFactory serverFactory = InProcessServerFactory.create();
    executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());
    ControlClientPool clientPool = MapControlClientPool.create();
    FnApiControlClientPoolService clientPoolService =
        FnApiControlClientPoolService.offeringClientsToPool(
            clientPool.getSink(), GrpcContextHeaderAccessorProvider.getHeaderAccessor());

    loggingServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcLoggingService.forWriter(Slf4jLogWriter.getDefault()), serverFactory);
    dataServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcDataService.create(
                PipelineOptionsFactory.create(), executor, OutboundObserverFactory.serverDirect()),
            serverFactory);
    controlServer = GrpcFnServer.allocatePortAndCreateFor(clientPoolService, serverFactory);

    InstructionRequestHandler requestHandler =
        EmbeddedEnvironmentFactory.create(
                PipelineOptionsFactory.create(),
                loggingServer,
                controlServer,
                clientPool.getSource())
            // The EmbeddedEnvironmentFactory can only create Java environments, regardless of the
            // Environment that's passed to it.
            .createEnvironment(Environment.getDefaultInstance(), "unusedWorkerId")
            .getInstructionRequestHandler();

    // TODO: https://issues.apache.org/jira/browse/BEAM-4149 Worker ids cannot currently be set by
    // the harness. All clients have the implicit empty id for now.
    client = SdkHarnessClient.usingFnApiClient(requestHandler, dataServer.getService());
  }

  @Override
  protected void after() {
    try (AutoCloseable logs = loggingServer;
        AutoCloseable data = dataServer;
        AutoCloseable ctl = controlServer;
        AutoCloseable c = client) {
      executor.shutdownNow();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
