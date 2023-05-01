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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.samza.config.ConfigLoaderFactory;
import org.apache.samza.config.loaders.PropertiesConfigLoaderFactory;
import org.apache.samza.metrics.MetricsReporter;

/** Options which can be used to configure a Samza PortablePipelineRunner. */
public interface SamzaPipelineOptions extends PipelineOptions {

  @Description(
      "The config file for Samza. It is *optional*. By default Samza supports properties config."
          + "Without a config file, Samza uses a default config for local execution.")
  String getConfigFilePath();

  void setConfigFilePath(String filePath);

  @Description("The factory to read config file from config file path.")
  @Default.Class(PropertiesConfigLoaderFactory.class)
  Class<? extends ConfigLoaderFactory> getConfigLoaderFactory();

  void setConfigLoaderFactory(Class<? extends ConfigLoaderFactory> configLoaderFactory);

  @Description(
      "The config override to set programmatically. It will be applied on "
          + "top of config file if it exits, otherwise used directly as the config.")
  Map<String, String> getConfigOverride();

  void setConfigOverride(Map<String, String> configs);

  @Description("The instance name of the job")
  @Default.String("1")
  String getJobInstance();

  void setJobInstance(String instance);

  @Description(
      "Samza application execution environment."
          + "See {@link org.apache.beam.runners.samza.SamzaExecutionEnvironment} for detailed environment descriptions.")
  @Default.Enum("LOCAL")
  SamzaExecutionEnvironment getSamzaExecutionEnvironment();

  void setSamzaExecutionEnvironment(SamzaExecutionEnvironment environment);

  @Description("The interval to check for watermarks in milliseconds.")
  @Default.Long(1000)
  long getWatermarkInterval();

  void setWatermarkInterval(long interval);

  @Description("The maximum number of messages to buffer for a given system.")
  @Default.Integer(5000)
  int getSystemBufferSize();

  void setSystemBufferSize(int consumerBufferSize);

  @Description("The maximum number of event-time timers to buffer in memory for a PTransform")
  @Default.Integer(50000)
  int getEventTimerBufferSize();

  void setEventTimerBufferSize(int eventTimerBufferSize);

  @Description("The maximum number of ready timers to process at once per watermark.")
  @Default.Integer(Integer.MAX_VALUE)
  int getMaxReadyTimersToProcessOnce();

  void setMaxReadyTimersToProcessOnce(int maxReadyTimersToProcessOnce);

  @Description("The maximum parallelism allowed for any data source.")
  @Default.Integer(1)
  int getMaxSourceParallelism();

  void setMaxSourceParallelism(int maxSourceParallelism);

  @Description("The batch get size limit for the state store.")
  @Default.Integer(10000)
  int getStoreBatchGetSize();

  void setStoreBatchGetSize(int storeBatchGetSize);

  @Description("Enable/disable Beam metrics in Samza Runner")
  @Default.Boolean(true)
  Boolean getEnableMetrics();

  void setEnableMetrics(Boolean enableMetrics);

  @Description("Enable/disable Beam Transform throughput, latency metrics in Samza Runner")
  @Default.Boolean(true)
  Boolean getEnableTransformMetrics();

  void setEnableTransformMetrics(Boolean enableMetrics);

  @Description("The config for state to be durable")
  @Default.Boolean(false)
  Boolean getStateDurable();

  void setStateDurable(Boolean stateDurable);

  @JsonIgnore
  @Description("The metrics reporters that will be used to emit metrics.")
  List<MetricsReporter> getMetricsReporters();

  void setMetricsReporters(List<MetricsReporter> reporters);

  @Description("The maximum number of elements in a bundle.")
  @Default.Long(1)
  long getMaxBundleSize();

  void setMaxBundleSize(long maxBundleSize);

  @Description("The maximum time to wait before finalising a bundle (in milliseconds).")
  @Default.Long(1000)
  long getMaxBundleTimeMs();

  void setMaxBundleTimeMs(long maxBundleTimeMs);

  @Description(
      "Wait if necessary for completing a remote bundle processing for at most the given time (in milliseconds). if the value of timeout is negative, wait forever until the bundle processing is completed. Used only in portable mode for now.")
  @Default.Long(-1)
  long getBundleProcessingTimeout();

  void setBundleProcessingTimeout(long timeoutMs);

  @Description(
      "The number of threads to run DoFn.processElements in parallel within a bundle. Used only in non-portable mode.")
  @Default.Integer(1)
  int getNumThreadsForProcessElement();

  void setNumThreadsForProcessElement(int numThreads);

  @JsonIgnore
  @Description(
      "The ExecutorService instance to run DoFN.processElements in parallel within a bundle. Used only in non-portable mode.")
  @Default.InstanceFactory(ProcessElementExecutorServiceFactory.class)
  @Hidden
  ExecutorService getExecutorServiceForProcessElement();

  void setExecutorServiceForProcessElement(ExecutorService executorService);

  class ProcessElementExecutorServiceFactory implements DefaultValueFactory<ExecutorService> {

    @Override
    public ExecutorService create(PipelineOptions options) {
      return Executors.newFixedThreadPool(
          options.as(SamzaPipelineOptions.class).getNumThreadsForProcessElement(),
          new ThreadFactoryBuilder().setNameFormat("Process Element Thread-%d").build());
    }
  }
}
