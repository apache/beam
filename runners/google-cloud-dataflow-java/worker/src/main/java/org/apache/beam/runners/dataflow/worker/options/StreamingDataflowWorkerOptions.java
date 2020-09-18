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
package org.apache.beam.runners.dataflow.worker.options;

import java.io.IOException;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.windmill.GrpcWindmillServer;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServer;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Duration;

/** [Internal] Options for configuring StreamingDataflowWorker. */
@Description("[Internal] Options for configuring StreamingDataflowWorker.")
@Hidden
public interface StreamingDataflowWorkerOptions extends DataflowWorkerHarnessOptions {
  @Description("Stub for communicating with Windmill.")
  @Default.InstanceFactory(WindmillServerStubFactory.class)
  WindmillServerStub getWindmillServerStub();

  void setWindmillServerStub(WindmillServerStub value);

  @Description("Hostport of a co-located Windmill server.")
  @Default.InstanceFactory(LocalWindmillHostportFactory.class)
  String getLocalWindmillHostport();

  void setLocalWindmillHostport(String value);

  @Description(
      "Period for reporting worker updates. The duration is specified as seconds in "
          + "'PTx.yS' format, e.g. 'PT5.125S'. Default is PT10S (10 seconds)."
          + "Explicitly set only in tests.")
  @Default.InstanceFactory(HarnessUpdateReportingPeriodFactory.class)
  Duration getWindmillHarnessUpdateReportingPeriod();

  void setWindmillHarnessUpdateReportingPeriod(Duration value);

  @Description("Limit on depth of user exception stack trace reported to cloud monitoring.")
  @Default.InstanceFactory(MaxStackTraceDepthToReportFactory.class)
  int getMaxStackTraceDepthToReport();

  void setMaxStackTraceDepthToReport(int value);

  @Description(
      "Frequency at which active work should be reported back to Windmill, in millis. "
          + "The first refresh will occur after at least this much time has passed since "
          + "starting the work item")
  @Default.Integer(10000)
  int getActiveWorkRefreshPeriodMillis();

  void setActiveWorkRefreshPeriodMillis(int value);

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
          + "and thread stacks to a file in this directory. Generally only set for tests.")
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

  @Description(
      "If positive, frequency at which windmill service streaming rpcs will have application "
          + "level health checks.")
  @Default.Integer(10000)
  int getWindmillServiceStreamingRpcHealthCheckPeriodMs();

  void setWindmillServiceStreamingRpcHealthCheckPeriodMs(int value);

  /**
   * Factory for creating local Windmill address. Reads from system propery 'windmill.hostport' for
   * backwards compatibility.
   */
  public static class LocalWindmillHostportFactory implements DefaultValueFactory<String> {
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
  static class HarnessUpdateReportingPeriodFactory implements DefaultValueFactory<Duration> {
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
  static class GlobalConfigRefreshPeriodFactory implements DefaultValueFactory<Duration> {
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
  static class MaxStackTraceDepthToReportFactory implements DefaultValueFactory<Integer> {
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
  static class PeriodicStatusPageDirectoryFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return System.getProperty("windmill.periodic_status_page_directory");
    }
  }

  /**
   * Factory for creating {@link WindmillServerStub} instances. If {@link setLocalWindmillHostport}
   * is set, returns a stub to a local Windmill server, otherwise returns a remote gRPC stub.
   */
  public static class WindmillServerStubFactory implements DefaultValueFactory<WindmillServerStub> {
    @Override
    public WindmillServerStub create(PipelineOptions options) {
      StreamingDataflowWorkerOptions streamingOptions =
          options.as(StreamingDataflowWorkerOptions.class);
      if (streamingOptions.getWindmillServiceEndpoint() != null
          || streamingOptions.isEnableStreamingEngine()
          || streamingOptions.getLocalWindmillHostport().startsWith("grpc:")) {
        try {
          return new GrpcWindmillServer(streamingOptions);
        } catch (IOException e) {
          throw new RuntimeException("Failed to create GrpcWindmillServer: ", e);
        }
      } else {
        return new WindmillServer(streamingOptions.getLocalWindmillHostport());
      }
    }
  }

  /** Factory for setting value of WindmillServiceStreamingRpcBatchLimit based on environment. */
  public static class WindmillServiceStreamingRpcBatchLimitFactory
      implements DefaultValueFactory<Integer> {
    @Override
    public Integer create(PipelineOptions options) {
      StreamingDataflowWorkerOptions streamingOptions =
          options.as(StreamingDataflowWorkerOptions.class);
      return streamingOptions.isEnableStreamingEngine() ? Integer.MAX_VALUE : 1;
    }
  }
}
