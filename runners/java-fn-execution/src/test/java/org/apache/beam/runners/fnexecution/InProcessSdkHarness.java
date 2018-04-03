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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.fnexecution.control.FnApiControlClient;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.stream.StreamObserverFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/**
 * A {@link TestRule} which creates a {@link FnHarness} in a thread, services required for that
 * {@link FnHarness} to properly execute, and provides access to the associated client and harness
 * during test execution.
 */
public class InProcessSdkHarness extends ExternalResource implements TestRule {

  public static InProcessSdkHarness create() {
    return new InProcessSdkHarness();
  }

  private ExecutorService executor;
  private GrpcFnServer<GrpcLoggingService> loggingServer;
  private GrpcFnServer<GrpcDataService> dataServer;
  private GrpcFnServer<FnApiControlClientPoolService> controlServer;

  private SdkHarnessClient client;

  private InProcessSdkHarness() {}

  public SdkHarnessClient client() {
    return client;
  }

  public ApiServiceDescriptor dataEndpoint() {
    return dataServer.getApiServiceDescriptor();
  }

  protected void before() throws IOException, InterruptedException {
    InProcessServerFactory serverFactory = InProcessServerFactory.create();
    executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());
    SynchronousQueue<FnApiControlClient> clientPool = new SynchronousQueue<>();
    FnApiControlClientPoolService clientPoolService =
        FnApiControlClientPoolService.offeringClientsToPool(
            clientPool, GrpcContextHeaderAccessorProvider.getHeaderAccessor());

    loggingServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcLoggingService.forWriter(Slf4jLogWriter.getDefault()), serverFactory);
    dataServer =
        GrpcFnServer.allocatePortAndCreateFor(GrpcDataService.create(executor), serverFactory);
    controlServer = GrpcFnServer.allocatePortAndCreateFor(clientPoolService, serverFactory);

    executor.submit(
        () -> {
          FnHarness.main(
              PipelineOptionsFactory.create(),
              loggingServer.getApiServiceDescriptor(),
              controlServer.getApiServiceDescriptor(),
              new ManagedChannelFactory() {
                @Override
                public ManagedChannel forDescriptor(ApiServiceDescriptor apiServiceDescriptor) {
                  return InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();
                }
              },
              StreamObserverFactory.direct());
          return null;
        });

    client = SdkHarnessClient.usingFnApiClient(clientPool.take(), dataServer.getService());
  }

  protected void after() {
    try (AutoCloseable logs = loggingServer;
        AutoCloseable data = dataServer;
        AutoCloseable ctl = controlServer;
        AutoCloseable c = client; ) {
      executor.shutdownNow();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
