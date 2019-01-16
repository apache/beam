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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.MapControlClientPool;
import org.apache.beam.runners.fnexecution.control.SingleEnvironmentInstanceJobBundleFactory;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.samza.context.ApplicationContainerContext;
import org.apache.samza.context.ApplicationContainerContextFactory;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runtime context for the Samza runner. */
public class SamzaExecutionContext implements ApplicationContainerContext {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaExecutionContext.class);
  private static final String SAMZA_WORKER_ID = "samza_py_worker_id";

  private final SamzaPipelineOptions options;
  private SamzaMetricsContainer metricsContainer;
  private JobBundleFactory jobBundleFactory;
  private GrpcFnServer<FnApiControlClientPoolService> fnControlServer;
  private GrpcFnServer<GrpcDataService> fnDataServer;
  private GrpcFnServer<GrpcStateService> fnStateServer;
  private ControlClientPool controlClientPool;
  private IdGenerator idGenerator = IdGenerators.incrementingLongs();

  public SamzaExecutionContext(SamzaPipelineOptions options) {
    this.options = options;
  }

  public SamzaPipelineOptions getPipelineOptions() {
    return options;
  }

  public SamzaMetricsContainer getMetricsContainer() {
    return this.metricsContainer;
  }

  void setMetricsContainer(SamzaMetricsContainer metricsContainer) {
    this.metricsContainer = metricsContainer;
  }

  public JobBundleFactory getJobBundleFactory() {
    return this.jobBundleFactory;
  }

  void setJobBundleFactory(JobBundleFactory jobBundleFactory) {
    this.jobBundleFactory = jobBundleFactory;
  }

  @Override
  public void start() {
    checkState(getJobBundleFactory() == null, "jobBundleFactory has been created!");

    if (SamzaRunnerOverrideConfigs.isPortableMode(options)) {
      try {
        controlClientPool = MapControlClientPool.create();
        final ExecutorService dataExecutor = Executors.newCachedThreadPool();

        fnControlServer =
            GrpcFnServer.allocatePortAndCreateFor(
                FnApiControlClientPoolService.offeringClientsToPool(
                    controlClientPool.getSink(), () -> SAMZA_WORKER_ID),
                ServerFactory.createWithPortSupplier(
                    () -> SamzaRunnerOverrideConfigs.getFnControlPort(options)));

        fnDataServer =
            GrpcFnServer.allocatePortAndCreateFor(
                GrpcDataService.create(dataExecutor, OutboundObserverFactory.serverDirect()),
                ServerFactory.createDefault());

        fnStateServer =
            GrpcFnServer.allocatePortAndCreateFor(
                GrpcStateService.create(), ServerFactory.createDefault());

        final long waitTimeoutMs =
            SamzaRunnerOverrideConfigs.getControlClientWaitTimeoutMs(options);
        final InstructionRequestHandler instructionHandler =
            controlClientPool.getSource().take(SAMZA_WORKER_ID, Duration.ofMillis(waitTimeoutMs));
        final EnvironmentFactory environmentFactory =
            environment -> RemoteEnvironment.forHandler(environment, instructionHandler);
        // TODO: use JobBundleFactoryBase.WrappedSdkHarnessClient.wrapping
        jobBundleFactory =
            SingleEnvironmentInstanceJobBundleFactory.create(
                environmentFactory, fnDataServer, fnStateServer, idGenerator);
      } catch (Exception e) {
        throw new RuntimeException(
            "Running samza in Beam portable mode but failed to create job bundle factory", e);
      }

      setJobBundleFactory(jobBundleFactory);
    }
  }

  @Override
  public void stop() {
    closeFnServer(fnControlServer);
    fnControlServer = null;
    closeFnServer(fnDataServer);
    fnDataServer = null;
    closeFnServer(fnStateServer);
    fnStateServer = null;
  }

  private void closeFnServer(GrpcFnServer<?> fnServer) {
    try (AutoCloseable closer = fnServer) {
      // do nothing
    } catch (Exception e) {
      LOG.error("Failed to close fn api servers. Ignore since this is shutdown process...", e);
    }
  }

  /** The factory to return this {@link SamzaExecutionContext}. */
  public class Factory implements ApplicationContainerContextFactory<SamzaExecutionContext> {

    @Override
    public SamzaExecutionContext create(
        ExternalContext externalContext, JobContext jobContext, ContainerContext containerContext) {

      final MetricsRegistryMap metricsRegistry =
          (MetricsRegistryMap) containerContext.getContainerMetricsRegistry();
      SamzaExecutionContext.this.setMetricsContainer(new SamzaMetricsContainer(metricsRegistry));
      return SamzaExecutionContext.this;
    }
  }
}
