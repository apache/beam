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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.UUID;
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
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.vendor.grpc.v1p26p0.io.netty.util.internal.StringUtil;
import org.apache.commons.io.FileUtils;
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
  private ExecutorService dataExecutor;
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

  private static ServerFactory createControlServerFactory(
      int controlPort, boolean useToken, String token) throws IOException {
    if (useToken) {
      return ServerFactory.createSecureLocalServerFactory(() -> controlPort, token);
    } else {
      return ServerFactory.createWithPortSupplier(() -> controlPort);
    }
  }

  private static ServerFactory createDataStateServerFactory(boolean useToken, String token)
      throws IOException {
    if (useToken) {
      return ServerFactory.createSecureLocalServerFactory(() -> 0, token);
    } else {
      return ServerFactory.createDefault();
    }
  }

  private static void writeFsTokenToFile(String tokenPath, String token) throws IOException {
    final File file = new File(tokenPath);
    if (!file.createNewFile()) {
      LOG.info("Fs token file already exists. Will override.");
    }
    Files.setPosixFilePermissions(file.toPath(), PosixFilePermissions.fromString("rw-------"));
    FileUtils.writeStringToFile(file, token, Charset.defaultCharset());
  }

  @Override
  public void start() {
    checkState(getJobBundleFactory() == null, "jobBundleFactory has been created!");

    if (SamzaRunnerOverrideConfigs.isPortableMode(options)) {
      try {
        controlClientPool = MapControlClientPool.create();
        dataExecutor = Executors.newCachedThreadPool();

        final String fsTokenPath = SamzaRunnerOverrideConfigs.getFsTokenPath(options);
        final String fsToken = UUID.randomUUID().toString(); // 128 bits
        final boolean useToken = !StringUtil.isNullOrEmpty(fsTokenPath);

        if (useToken) {
          LOG.info("Creating secure (auth-enabled) channels with fs token path: {}", fsTokenPath);
          writeFsTokenToFile(fsTokenPath, fsToken);
        } else {
          LOG.info("Fs token path not provided. Will create channels in insecure mode.");
        }

        final ServerFactory controlServerFactory =
            createControlServerFactory(
                SamzaRunnerOverrideConfigs.getFnControlPort(options), useToken, fsToken);
        final ServerFactory dataStateServerFactory =
            createDataStateServerFactory(useToken, fsToken);

        fnControlServer =
            GrpcFnServer.allocatePortAndCreateFor(
                FnApiControlClientPoolService.offeringClientsToPool(
                    controlClientPool.getSink(), () -> SAMZA_WORKER_ID),
                controlServerFactory);
        LOG.info("Started control server on port {}", fnControlServer.getServer().getPort());

        fnDataServer =
            GrpcFnServer.allocatePortAndCreateFor(
                GrpcDataService.create(
                    options, dataExecutor, OutboundObserverFactory.serverDirect()),
                dataStateServerFactory);
        LOG.info("Started data server on port {}", fnDataServer.getServer().getPort());

        fnStateServer =
            GrpcFnServer.allocatePortAndCreateFor(
                GrpcStateService.create(), dataStateServerFactory);
        LOG.info("Started state server on port {}", fnStateServer.getServer().getPort());

        final long waitTimeoutMs =
            SamzaRunnerOverrideConfigs.getControlClientWaitTimeoutMs(options);
        LOG.info("Control client wait timeout config: " + waitTimeoutMs);

        final InstructionRequestHandler instructionHandler =
            controlClientPool.getSource().take(SAMZA_WORKER_ID, Duration.ofMillis(waitTimeoutMs));
        final EnvironmentFactory environmentFactory =
            (environment, workerId) ->
                RemoteEnvironment.forHandler(environment, instructionHandler);
        // TODO: use JobBundleFactoryBase.WrappedSdkHarnessClient.wrapping
        jobBundleFactory =
            SingleEnvironmentInstanceJobBundleFactory.create(
                environmentFactory, fnDataServer, fnStateServer, idGenerator);
        LOG.info("Started job bundle factory");
      } catch (Exception e) {
        throw new RuntimeException(
            "Running samza in Beam portable mode but failed to create job bundle factory", e);
      }

      setJobBundleFactory(jobBundleFactory);
    }
  }

  @Override
  public void stop() {
    closeAutoClosable(fnControlServer, "controlServer");
    fnControlServer = null;
    closeAutoClosable(fnDataServer, "dataServer");
    fnDataServer = null;
    closeAutoClosable(fnStateServer, "stateServer");
    fnStateServer = null;
    if (dataExecutor != null) {
      dataExecutor.shutdown();
      dataExecutor = null;
    }
    controlClientPool = null;
    closeAutoClosable(jobBundleFactory, "jobBundle");
    jobBundleFactory = null;
  }

  private static void closeAutoClosable(AutoCloseable closeable, String name) {
    try (AutoCloseable closer = closeable) {
      LOG.info("Closed {}", name);
    } catch (Exception e) {
      LOG.error(
          "Failed to close {}. Ignore since this is shutdown process...",
          closeable.getClass().getSimpleName(),
          e);
    }
  }

  /** The factory to return this {@link SamzaExecutionContext}. */
  public class Factory implements ApplicationContainerContextFactory<SamzaExecutionContext> {

    @Override
    public SamzaExecutionContext create(
        ExternalContext externalContext, JobContext jobContext, ContainerContext containerContext) {

      final MetricsRegistryMap metricsRegistry =
          (MetricsRegistryMap) containerContext.getContainerMetricsRegistry();
      SamzaMetricsContainer samzaMetricsContainer =
          new SamzaMetricsContainer(metricsRegistry, jobContext.getConfig());
      MetricsEnvironment.setGlobalContainer(
          samzaMetricsContainer.getContainer(SamzaMetricsContainer.GLOBAL_CONTAINER_STEP_NAME));
      SamzaExecutionContext.this.setMetricsContainer(samzaMetricsContainer);
      return SamzaExecutionContext.this;
    }
  }
}
