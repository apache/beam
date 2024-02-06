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
package org.apache.beam.runners.dataflow.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.Security;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.ExperimentContext.Experiment;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.conscrypt.OpenSSLProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper class for initialization of the Dataflow worker harness. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class DataflowWorkerHarnessHelper {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowWorkerHarnessHelper.class);

  private static final String CONTROL_API_SERVICE_DESCRIPTOR = "CONTROL_API_SERVICE_DESCRIPTOR";
  private static final String LOGGING_API_SERVICE_DESCRIPTOR = "LOGGING_API_SERVICE_DESCRIPTOR";
  private static final String STATUS_API_SERVICE_DESCRIPTOR = "STATUS_API_SERVICE_DESCRIPTOR";
  private static final String ROOT_LOGGER_NAME = "";
  private static final String PIPELINE_PATH = "PIPELINE_PATH";

  public static <T extends DataflowWorkerHarnessOptions> T initializeGlobalStateAndPipelineOptions(
      Class<?> workerHarnessClass, Class<T> harnessOptionsClass) throws Exception {
    /* Extract pipeline options. */
    T pipelineOptions =
        WorkerPipelineOptionsFactory.createFromSystemProperties(harnessOptionsClass);
    pipelineOptions.setAppName(workerHarnessClass.getSimpleName());

    /* Configure logging with job-specific properties. */
    DataflowWorkerLoggingMDC.setJobId(pipelineOptions.getJobId());
    DataflowWorkerLoggingMDC.setWorkerId(pipelineOptions.getWorkerId());

    ExperimentContext ec = ExperimentContext.parseFrom(pipelineOptions);

    String experimentName = Experiment.EnableConscryptSecurityProvider.getName();
    if (ec.isEnabled(Experiment.EnableConscryptSecurityProvider)) {
      /* Enable fast SSL provider. */
      LOG.info(
          "Dataflow runner is using conscrypt SSL. To disable this feature, "
              + "remove the pipeline option --experiments={}",
          experimentName);
      Security.insertProviderAt(new OpenSSLProvider(), 1);
    } else {
      LOG.info(
          "Not using conscrypt SSL. Note this is the default Java behavior, but may "
              + "have reduced performance. To use conscrypt SSL pass pipeline option "
              + "--experiments={}",
          experimentName);
    }
    return pipelineOptions;
  }

  @SuppressWarnings("Slf4jIllegalPassedClass")
  public static void initializeLogging(Class<?> workerHarnessClass) {
    /* Set up exception handling tied to the workerHarnessClass. */
    Thread.setDefaultUncaughtExceptionHandler(
        new WorkerUncaughtExceptionHandler(LoggerFactory.getLogger(workerHarnessClass)));

    // Reset the global log manager, get the root logger and remove the default log handlers.
    LogManager logManager = LogManager.getLogManager();
    logManager.reset();
    java.util.logging.Logger rootLogger = logManager.getLogger(ROOT_LOGGER_NAME);
    for (Handler handler : rootLogger.getHandlers()) {
      rootLogger.removeHandler(handler);
    }
    DataflowWorkerLoggingInitializer.initialize();
  }

  public static void configureLogging(DataflowWorkerHarnessOptions pipelineOptions) {

    DataflowWorkerLoggingInitializer.configure(pipelineOptions);
  }

  public static Endpoints.ApiServiceDescriptor parseApiServiceDescriptorFromText(
      String descriptorText) throws TextFormat.ParseException {
    Endpoints.ApiServiceDescriptor.Builder apiServiceDescriptorBuilder =
        Endpoints.ApiServiceDescriptor.newBuilder();
    TextFormat.merge(descriptorText, apiServiceDescriptorBuilder);
    return apiServiceDescriptorBuilder.build();
  }

  public static Endpoints.ApiServiceDescriptor getLoggingDescriptor()
      throws TextFormat.ParseException {
    return parseApiServiceDescriptorFromText(System.getenv().get(LOGGING_API_SERVICE_DESCRIPTOR));
  }

  public static Endpoints.ApiServiceDescriptor getControlDescriptor()
      throws TextFormat.ParseException {
    return parseApiServiceDescriptorFromText(System.getenv().get(CONTROL_API_SERVICE_DESCRIPTOR));
  }

  public static Endpoints.@Nullable ApiServiceDescriptor getStatusDescriptor()
      throws TextFormat.ParseException {
    String statusApiDescriptor = System.getenv().get(STATUS_API_SERVICE_DESCRIPTOR);
    if (Strings.isNullOrEmpty(statusApiDescriptor)) {
      // Missing STATUS_API_SERVICE_DESCRIPTOR env var is a signal that the worker status API
      // is unsupported by the current runner.
      return null;
    }
    return parseApiServiceDescriptorFromText(statusApiDescriptor);
  }

  // TODO: make env logic private to main() so it is never done outside of initializing the process
  public static RunnerApi.@Nullable Pipeline getPipelineFromEnv() throws IOException {
    String pipelinePath = System.getenv(PIPELINE_PATH);
    if (pipelinePath == null) {
      LOG.warn("Missing pipeline environment variable '{}'", PIPELINE_PATH);
      return null;
    }

    File pipelineFile = new File(System.getenv(PIPELINE_PATH));
    if (!pipelineFile.exists()) {
      LOG.warn("Pipeline path '{}' does not exist", pipelineFile);
      return null;
    }

    try (FileInputStream inputStream = new FileInputStream(pipelineFile)) {
      RunnerApi.Pipeline pipelineProto = RunnerApi.Pipeline.parseFrom(inputStream);
      LOG.info("Found portable pipeline:\n{}", TextFormat.printer().printToString(pipelineProto));
      return pipelineProto;
    }
  }
}
