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
package org.apache.beam.runners.dataflow.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Duration;

/** [Internal] Options for configuring StreamingDataflowWorker. */
@Description("[Internal] Options for configuring StreamingDataflowWorker.")
@Hidden
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public interface DataflowStreamingPipelineOptions extends PipelineOptions {

  /** Custom windmill_main binary to use with the streaming runner. */
  @Description("Custom windmill_main binary to use with the streaming runner")
  String getOverrideWindmillBinary();

  void setOverrideWindmillBinary(String value);

  /** Custom windmill service endpoint. */
  @Description("Custom windmill service endpoint.")
  String getWindmillServiceEndpoint();

  void setWindmillServiceEndpoint(String value);

  @Description("Port for communicating with a remote windmill service.")
  @Default.Integer(443)
  int getWindmillServicePort();

  void setWindmillServicePort(int value);

  @Description("Hostport of a co-located Windmill server.")
  @Default.InstanceFactory(LocalWindmillHostportFactory.class)
  String getLocalWindmillHostport();

  void setLocalWindmillHostport(String value);

  /**
   * Maximum number of bundles outstanding from windmill before the worker stops requesting.
   *
   * <p>If <= 0, use the default value of 100 + getNumberOfWorkerHarnessThreads()
   */
  @Description(
      "Maximum number of bundles outstanding from windmill before the worker stops requesting.")
  @Default.Integer(0)
  int getMaxBundlesFromWindmillOutstanding();

  void setMaxBundlesFromWindmillOutstanding(int value);

  /**
   * Maximum number of bytes outstanding from windmill before the worker stops requesting.
   *
   * <p>If <= 0, use the default value of 50% of jvm memory.
   */
  @Description(
      "Maximum number of bytes outstanding from windmill before the worker stops requesting. If <= 0, use the default value of 50% of jvm memory.")
  @Default.Long(0)
  long getMaxBytesFromWindmillOutstanding();

  void setMaxBytesFromWindmillOutstanding(long value);

  @Description("The size of the streaming worker's side input cache, in megabytes.")
  @Default.Integer(100)
  Integer getStreamingSideInputCacheMb();

  void setStreamingSideInputCacheMb(Integer value);

  @Description("The expiry for streaming worker's side input cache entries, in milliseconds.")
  @Default.Integer(60 * 1000) // 1 minute
  Integer getStreamingSideInputCacheExpirationMillis();

  void setStreamingSideInputCacheExpirationMillis(Integer value);

  @Description("Number of commit threads used to commit items to streaming engine.")
  @Default.Integer(1)
  Integer getWindmillServiceCommitThreads();

  void setWindmillServiceCommitThreads(Integer value);

  @Description(
      "Frequency at which active work should be reported back to Windmill, in millis. "
          + "The first refresh will occur after at least this much time has passed since "
          + "starting the work item")
  @Default.Integer(10000)
  int getActiveWorkRefreshPeriodMillis();

  void setActiveWorkRefreshPeriodMillis(int value);

  @Description(
      "If positive, frequency at which windmill service streaming rpcs will have application "
          + "level health checks.")
  @Default.Integer(10000)
  int getWindmillServiceStreamingRpcHealthCheckPeriodMs();

  void setWindmillServiceStreamingRpcHealthCheckPeriodMs(int value);

  @Description(
      "If positive, the number of messages to send on streaming rpc before checking isReady."
          + "Higher values reduce cost of output overhead at the cost of more memory used in grpc "
          + "buffers.")
  @Default.Integer(10)
  int getWindmillMessagesBetweenIsReadyChecks();

  void setWindmillMessagesBetweenIsReadyChecks(int value);

  @Description("If true, a most a single active rpc will be used per channel.")
  Boolean getUseWindmillIsolatedChannels();

  void setUseWindmillIsolatedChannels(Boolean value);

  @Description(
      "If true, separate streaming rpcs will be used for heartbeats instead of sharing streams with state reads.")
  Boolean getUseSeparateWindmillHeartbeatStreams();

  void setUseSeparateWindmillHeartbeatStreams(Boolean value);

  @Description("The number of streams to use for GetData requests.")
  @Default.Integer(1)
  int getWindmillGetDataStreamCount();

  void setWindmillGetDataStreamCount(int value);

  @Description("If true, will only show windmill service channels on /channelz")
  @Default.Boolean(true)
  boolean getChannelzShowOnlyWindmillServiceChannels();

  void setChannelzShowOnlyWindmillServiceChannels(boolean value);

  @Description(
      "Period for reporting worker updates. The duration is specified as seconds in "
          + "'PTx.yS' format, e.g. 'PT5.125S'. Default is PT10S (10 seconds)."
          + "Explicitly set only in tests.")
  @Default.InstanceFactory(HarnessUpdateReportingPeriodFactory.class)
  Duration getWindmillHarnessUpdateReportingPeriod();

  void setWindmillHarnessUpdateReportingPeriod(Duration value);

  @Description(
      "Specifies how often system defined per-worker metrics are reported. These metrics are "
          + " reported on the worker updates path so this number will be rounded up to the "
          + " nearest multiple of WindmillHarnessUpdateReportingPeriod. If that value is 0, then "
          + " these metrics are never sent.")
  @Default.Integer(30000)
  int getPerWorkerMetricsUpdateReportingPeriodMillis();

  void setPerWorkerMetricsUpdateReportingPeriodMillis(int value);

  @Description("Limit on depth of user exception stack trace reported to cloud monitoring.")
  @Default.InstanceFactory(MaxStackTraceDepthToReportFactory.class)
  int getMaxStackTraceDepthToReport();

  void setMaxStackTraceDepthToReport(int value);

  @Description("Necessary duration for a commit to be considered stuck and invalidated.")
  @Default.Integer(10 * 60 * 1000)
  int getStuckCommitDurationMillis();

  void setStuckCommitDurationMillis(int value);

  @Description(
      "Period for sending 'global get config' requests to the service. The duration is "
          + "specified as seconds in 'PTx.yS' format, e.g. 'PT5.125S'."
          + " Default is PT120S (2 minutes).")
  @Default.InstanceFactory(GlobalConfigRefreshPeriodFactory.class)
  Duration getGlobalConfigRefreshPeriod();

  void setGlobalConfigRefreshPeriod(Duration value);

  @Description(
      "If non-null, StreamingDataflowWorkerHarness will periodically snapshot it's status pages"
          + " and thread stacks to a file in this directory. Generally only set for tests.")
  @Default.InstanceFactory(PeriodicStatusPageDirectoryFactory.class)
  String getPeriodicStatusPageOutputDirectory();

  void setPeriodicStatusPageOutputDirectory(String directory);

  @Description("Streaming requests will be batched into messages up to this limit.")
  @Default.InstanceFactory(WindmillServiceStreamingRpcBatchLimitFactory.class)
  int getWindmillServiceStreamingRpcBatchLimit();

  void setWindmillServiceStreamingRpcBatchLimit(int value);

  @Description("Log streaming rpc errors once out of every N.")
  @Default.Integer(20)
  int getWindmillServiceStreamingLogEveryNStreamFailures();

  void setWindmillServiceStreamingLogEveryNStreamFailures(int value);

  @Description("The health check period and timeout for grpc channel healthchecks")
  @Default.Integer(40)
  int getWindmillServiceRpcChannelAliveTimeoutSec();

  void setWindmillServiceRpcChannelAliveTimeoutSec(int value);

  @Description("Max backoff with which the windmill service stream failures are retried")
  @Default.Integer(30 * 1000) // 30s
  int getWindmillServiceStreamMaxBackoffMillis();

  void setWindmillServiceStreamMaxBackoffMillis(int value);

  @Description("Enables direct path mode for streaming engine.")
  @Default.InstanceFactory(EnableWindmillServiceDirectPathFactory.class)
  boolean getIsWindmillServiceDirectPathEnabled();

  void setIsWindmillServiceDirectPathEnabled(boolean isWindmillServiceDirectPathEnabled);

  /**
   * Factory for creating local Windmill address. Reads from system propery 'windmill.hostport' for
   * backwards compatibility.
   */
  class LocalWindmillHostportFactory implements DefaultValueFactory<String> {
    private static final String WINDMILL_HOSTPORT_PROPERTY = "windmill.hostport";

    @Override
    public String create(PipelineOptions options) {
      return System.getProperty(WINDMILL_HOSTPORT_PROPERTY);
    }
  }

  /**
   * Read counter reporting period from system property 'windmill.harness_update_reporting_period'.
   * The duration is specified as seconds in "PTx.yS" format, e.g. 'PT2.153S'. @See Duration#parse
   */
  class HarnessUpdateReportingPeriodFactory implements DefaultValueFactory<Duration> {
    @Override
    public Duration create(PipelineOptions options) {
      Duration period =
          Duration.parse(System.getProperty("windmill.harness_update_reporting_period", "PT10S"));
      return period.isLongerThan(Duration.ZERO) ? period : Duration.standardSeconds(10);
    }
  }

  /**
   * Read global get config request period from system property
   * 'windmill.global_config_refresh_period'. The duration is specified as seconds in "PTx.yS"
   * format, e.g. 'PT2.153S'. @See Duration#parse
   */
  class GlobalConfigRefreshPeriodFactory implements DefaultValueFactory<Duration> {
    @Override
    public Duration create(PipelineOptions options) {
      Duration period =
          Duration.parse(System.getProperty("windmill.global_get_config_refresh_period", "PT120S"));
      return period.isLongerThan(Duration.ZERO) ? period : Duration.standardMinutes(2);
    }
  }

  /**
   * Read 'MaxStackTraceToReport' from system property 'windmill.max_stack_trace_to_report' or
   * Integer.MAX_VALUE if unspecified.
   */
  class MaxStackTraceDepthToReportFactory implements DefaultValueFactory<Integer> {
    @Override
    public Integer create(PipelineOptions options) {
      return Integer.parseInt(
          System.getProperty(
              "windmill.max_stack_trace_depth_to_report", Integer.toString(Integer.MAX_VALUE)));
    }
  }

  /**
   * Read 'PeriodicStatusPageOutputDirector' from system property
   * 'windmill.periodic_status_page_directory' or null if unspecified.
   */
  class PeriodicStatusPageDirectoryFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return System.getProperty("windmill.periodic_status_page_directory");
    }
  }

  /** Factory for setting value of WindmillServiceStreamingRpcBatchLimit based on environment. */
  class WindmillServiceStreamingRpcBatchLimitFactory implements DefaultValueFactory<Integer> {
    @Override
    public Integer create(PipelineOptions options) {
      DataflowWorkerHarnessOptions streamingOptions =
          options.as(DataflowWorkerHarnessOptions.class);
      return streamingOptions.isEnableStreamingEngine() ? Integer.MAX_VALUE : 1;
    }
  }

  /** EnableStreamingEngine defaults to false unless one of the two experiments is set. */
  class EnableWindmillServiceDirectPathFactory implements DefaultValueFactory<Boolean> {
    @Override
    public Boolean create(PipelineOptions options) {
      return ExperimentalOptions.hasExperiment(options, "enable_windmill_service_direct_path");
    }
  }
}
