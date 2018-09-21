/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.beam.runners.fnexecution.environment;

import static com.google.common.base.Preconditions.checkArgument;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.ControlClientPool.Source;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.InProcessManagedChannelFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EnvironmentFactory} that communicates to a {@link FnHarness} which is executing in the
 * same process.
 */
public class InProcessEnvironmentFactory implements EnvironmentFactory {
  private static final Logger LOG = LoggerFactory.getLogger(InProcessEnvironmentFactory.class);

  private final PipelineOptions options;

  private final GrpcFnServer<GrpcLoggingService> loggingServer;
  private final GrpcFnServer<FnApiControlClientPoolService> controlServer;

  private final ControlClientPool.Source clientSource;

  public static EnvironmentFactory create(
      PipelineOptions options,
      GrpcFnServer<GrpcLoggingService> loggingServer,
      GrpcFnServer<FnApiControlClientPoolService> controlServer,
      ControlClientPool.Source clientSource) {
    return new InProcessEnvironmentFactory(options, loggingServer, controlServer, clientSource);
  }

  private InProcessEnvironmentFactory(
      PipelineOptions options,
      GrpcFnServer<GrpcLoggingService> loggingServer,
      GrpcFnServer<FnApiControlClientPoolService> controlServer,
      Source clientSource) {
    this.options = options;
    this.loggingServer = loggingServer;
    this.controlServer = controlServer;
    checkArgument(
        loggingServer.getApiServiceDescriptor() != null,
        "Logging Server cannot have a null %s",
        ApiServiceDescriptor.class.getSimpleName());
    checkArgument(
        controlServer.getApiServiceDescriptor() != null,
        "Control Server cannot have a null %s",
        ApiServiceDescriptor.class.getSimpleName());
    this.clientSource = clientSource;
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored") // no need to monitor shutdown thread
  public RemoteEnvironment createEnvironment(Environment environment) throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> fnHarness =
        executor.submit(
            () -> {
              try {
                FnHarness.main(
                    "id",
                    options,
                    loggingServer.getApiServiceDescriptor(),
                    controlServer.getApiServiceDescriptor(),
                    InProcessManagedChannelFactory.create(),
                    OutboundObserverFactory.clientDirect());
              } catch (NoClassDefFoundError e) {
                // TODO: https://issues.apache.org/jira/browse/BEAM-4384 load the FnHarness in a
                // Restricted classpath that we control for any user.
                LOG.error(
                    "{} while executing an in-process FnHarness. "
                        + "To use the {}, "
                        + "the 'org.apache.beam:beam-sdks-java-harness' artifact "
                        + "and its dependencies must be on the classpath",
                    NoClassDefFoundError.class.getSimpleName(),
                    InProcessEnvironmentFactory.class.getSimpleName(),
                    e);
                throw e;
              }
              return null;
            });
    executor.submit(
        () -> {
          try {
            fnHarness.get();
          } catch (Throwable t) {
            executor.shutdownNow();
          }
        });

    // TODO: find some way to populate the actual ID in FnHarness.main()
    InstructionRequestHandler handler = clientSource.take("", Duration.ofMinutes(1L));
    return RemoteEnvironment.forHandler(environment, handler);
  }
}
