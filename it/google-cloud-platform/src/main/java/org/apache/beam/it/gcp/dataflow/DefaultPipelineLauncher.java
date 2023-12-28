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
package org.apache.beam.it.gcp.dataflow;

import static org.apache.beam.it.common.logging.LogStrings.formatForLogging;
import static org.apache.beam.sdk.testing.TestPipeline.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.gcp.IOLoadTestBase;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default class for implementation of {@link PipelineLauncher} interface. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/27438)
})
public class DefaultPipelineLauncher extends AbstractPipelineLauncher {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPipelineLauncher.class);
  private static final String READ_PIPELINE_NAME_OVERWRITE = "readPipelineNameOverride";
  private static final String WRITE_PIPELINE_NAME_OVERWRITE = "writePipelineNameOverride";
  private static final Pattern JOB_ID_PATTERN = Pattern.compile("Submitted job: (\\S+)");

  // For unsupported runners (other than dataflow), implement launcher methods by operating with
  // PipelineResult.
  private static final Map<String, PipelineResult> MANAGED_JOBS = new HashMap<>();

  // For supported runners (e.g. DataflowRunner), still keep a PipelineResult for pipeline specific
  // usages, e.g.,
  // polling custom metrics
  private static final Map<String, PipelineResult> UNMANAGED_JOBS = new HashMap<>();

  private static final long UNKNOWN_METRIC_VALUE = -1L;

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  private static final Map<PipelineResult.State, JobState> PIPELINE_STATE_TRANSLATE =
      ImmutableMap.<PipelineResult.State, JobState>builder()
          .put(PipelineResult.State.CANCELLED, JobState.CANCELLED)
          .put(PipelineResult.State.RUNNING, JobState.RUNNING)
          .put(PipelineResult.State.DONE, JobState.DONE)
          .put(PipelineResult.State.FAILED, JobState.FAILED)
          .put(PipelineResult.State.STOPPED, JobState.STOPPED)
          .put(PipelineResult.State.UNKNOWN, JobState.UNKNOWN)
          .put(PipelineResult.State.UPDATED, JobState.UPDATED)
          .put(PipelineResult.State.UNRECOGNIZED, JobState.UNKNOWN)
          .build();

  private DefaultPipelineLauncher(Builder builder) {
    super(
        new Dataflow(
            Utils.getDefaultTransport(),
            Utils.getDefaultJsonFactory(),
            builder.getCredentials() == null
                ? null
                : new HttpCredentialsAdapter(builder.getCredentials())));
  }

  public static Builder builder(Credentials credentials) {
    return new Builder(credentials);
  }

  @Override
  public JobState getJobStatus(String project, String region, String jobId) throws IOException {
    if (MANAGED_JOBS.containsKey(jobId)) {
      return PIPELINE_STATE_TRANSLATE.get(MANAGED_JOBS.get(jobId).getState());
    } else {
      return super.handleJobState(getJob(project, region, jobId));
    }
  }

  @Override
  public Job cancelJob(String project, String region, String jobId) {
    if (MANAGED_JOBS.containsKey(jobId)) {
      try {
        MANAGED_JOBS.get(jobId).cancel();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return new Job().setId(jobId).setRequestedState(JobState.CANCELLED.toString());
    } else {
      return super.cancelJob(project, region, jobId);
    }
  }

  @Override
  public Job getJob(String project, String region, String jobId) throws IOException {
    if (MANAGED_JOBS.containsKey(jobId)) {
      return new Job()
          .setId(jobId)
          .setRequestedState(
              PIPELINE_STATE_TRANSLATE.get(MANAGED_JOBS.get(jobId).getState()).toString());
    } else {
      return super.getJob(project, region, jobId);
    }
  }

  @Override
  public Job drainJob(String project, String region, String jobId) {
    if (MANAGED_JOBS.containsKey(jobId)) {
      // drain unsupported. Just cancel.
      Job job = new Job().setId(jobId).setRequestedState(JobState.DRAINED.toString());
      cancelJob(project, region, jobId);
      return job;
    } else {
      return super.drainJob(project, region, jobId);
    }
  }

  private static <T> void checkIfMetricResultIsUnique(
      String name, Iterable<MetricResult<T>> metricResult) throws IllegalStateException {
    int resultCount = Iterables.size(metricResult);
    Preconditions.checkState(
        resultCount <= 1,
        "More than one metric result matches name: %s in namespace %s. Metric results count: %s",
        name,
        IOLoadTestBase.BEAM_METRICS_NAMESPACE,
        resultCount);
  }

  private static Iterable<MetricResult<DistributionResult>> getDistributions(
      PipelineResult result, String metricName) {
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(IOLoadTestBase.BEAM_METRICS_NAMESPACE, metricName))
                    .build());
    return metrics.getDistributions();
  }

  /** Pull Beam pipeline defined metrics given the jobId. */
  public Long getBeamMetric(
      String jobId, IOLoadTestBase.PipelineMetricsType metricType, String metricName) {
    PipelineResult pipelineResult =
        MANAGED_JOBS.getOrDefault(jobId, UNMANAGED_JOBS.getOrDefault(jobId, null));
    if (pipelineResult != null) {
      MetricQueryResults metrics =
          pipelineResult
              .metrics()
              .queryMetrics(
                  MetricsFilter.builder()
                      .addNameFilter(
                          MetricNameFilter.named(IOLoadTestBase.BEAM_METRICS_NAMESPACE, metricName))
                      .build());

      switch (metricType) {
        case COUNTER:
          Iterable<MetricResult<Long>> counters = metrics.getCounters();
          checkIfMetricResultIsUnique(metricName, counters);
          try {
            MetricResult<Long> metricResult = counters.iterator().next();
            return metricResult.getAttempted();
          } catch (NoSuchElementException e) {
            LOG.error(
                "Failed to get metric {}, from namespace {}",
                metricName,
                IOLoadTestBase.BEAM_METRICS_NAMESPACE);
          }
          return UNKNOWN_METRIC_VALUE;
        case STARTTIME:
        case ENDTIME:
        case RUNTIME:
          Iterable<MetricResult<DistributionResult>> distributions =
              getDistributions(pipelineResult, metricName);
          Long lowestMin =
              StreamSupport.stream(distributions.spliterator(), true)
                  .map(element -> Objects.requireNonNull(element.getAttempted()).getMin())
                  .min(Long::compareTo)
                  .orElse(UNKNOWN_METRIC_VALUE);
          Long greatestMax =
              StreamSupport.stream(distributions.spliterator(), true)
                  .map(element -> Objects.requireNonNull(element.getAttempted()).getMax())
                  .max(Long::compareTo)
                  .orElse(UNKNOWN_METRIC_VALUE);
          if (metricType == IOLoadTestBase.PipelineMetricsType.STARTTIME) {
            return lowestMin;
          } else if (metricType == IOLoadTestBase.PipelineMetricsType.ENDTIME) {
            return greatestMax;
          } else {
            if (lowestMin != UNKNOWN_METRIC_VALUE && greatestMax != UNKNOWN_METRIC_VALUE) {
              return greatestMax - lowestMin;
            } else {
              return UNKNOWN_METRIC_VALUE;
            }
          }
        default:
          throw new IllegalArgumentException(
              String.format("Unexpected metric type %s.", metricType));
      }
    } else {
      LOG.warn("Query pipeline defined metrics this SDK or runner is currently unsupported.");
      return UNKNOWN_METRIC_VALUE;
    }
  }

  @Override
  public Double getMetric(String project, String region, String jobId, String metricName)
      throws IOException {
    if (metricName.startsWith(IOLoadTestBase.BEAM_METRICS_NAMESPACE)) {
      String[] nameSpacedMetrics = metricName.split(":", 3);
      Preconditions.checkState(
          nameSpacedMetrics.length == 3,
          String.format(
              "Invalid Beam metrics name: %s, expected: '%s:metric_type:metric_name'",
              metricName, IOLoadTestBase.BEAM_METRICS_NAMESPACE));
      IOLoadTestBase.PipelineMetricsType metricType =
          IOLoadTestBase.PipelineMetricsType.valueOf(nameSpacedMetrics[1]);

      // Pipeline defined metrics are long values. Have to cast to double that is what the base
      // class defined.
      return getBeamMetric(jobId, metricType, nameSpacedMetrics[2]).doubleValue();
    } else {
      return super.getMetric(project, region, jobId, metricName);
    }
  }

  @Override
  public Map<String, Double> getMetrics(String project, String region, String jobId)
      throws IOException {
    if (MANAGED_JOBS.containsKey(jobId)) {
      // unsupported. Just return an empty map
      return new HashMap<>();
    } else {
      return super.getMetrics(project, region, jobId);
    }
  }

  @Override
  public LaunchInfo launch(String project, String region, LaunchConfig options) throws IOException {
    checkState(
        options.sdk() != null,
        "Cannot launch a dataflow job "
            + "without sdk specified. Please specify sdk and try again!");
    LOG.info("Getting ready to launch {} in {} under {}", options.jobName(), region, project);
    LOG.info("Using parameters:\n{}", formatForLogging(options.parameters()));
    // Create SDK specific command and execute to launch dataflow job
    List<String> cmd = new ArrayList<>();
    String jobId;
    switch (options.sdk()) {
      case JAVA:
        checkState(
            options.pipeline() != null,
            "Cannot launch a dataflow job "
                + "without pipeline specified. Please specify pipeline and try again!");
        PipelineOptions pipelineOptions = options.pipeline().getOptions();
        if ("DataflowRunner".equalsIgnoreCase(options.getParameter("runner"))) {
          List<String> optionFromConfig = extractOptions(project, region, options);
          // a few options need to be set in pipeline expansion time, so they need to be preserved
          // here.
          // known options included: --streaming(expansion depends on) --tempLocation(validation
          // depends on)
          if (pipelineOptions.as(StreamingOptions.class).isStreaming()) {
            optionFromConfig.add("--streaming");
          }
          if (!Strings.isNullOrEmpty(pipelineOptions.getTempLocation())) {
            optionFromConfig.add(
                String.format("--tempLocation=%s", pipelineOptions.getTempLocation()));
          }

          // dataflow runner specific options
          PipelineOptions updatedOptions =
              PipelineOptionsFactory.fromArgs(optionFromConfig.toArray(new String[] {})).create();
          updatedOptions.setJobName(options.jobName());
          PipelineResult pipelineResult = options.pipeline().run(updatedOptions);
          // dataflow runner generated a jobId of certain format for each job
          DataflowPipelineJob job = (DataflowPipelineJob) pipelineResult;
          jobId = job.getJobId();
          UNMANAGED_JOBS.put(jobId, pipelineResult);
          launchedJobs.add(jobId);
        } else {

          pipelineOptions.setRunner(PipelineUtils.getRunnerClass(options.getParameter("runner")));
          pipelineOptions.setJobName(options.jobName());
          // for unsupported runners (e.g. direct runner) runner, manually record job properties
          Map<String, String> jobProperties = new HashMap<>();
          jobProperties.put(
              "createTime", Timestamps.toString(Timestamps.fromMillis(System.currentTimeMillis())));
          if (pipelineOptions.as(StreamingOptions.class).isStreaming()) {
            jobProperties.put("jobType", "JOB_TYPE_STREAMING");
          } else {
            jobProperties.put("jobType", "JOB_TYPE_BATCH");
          }
          PipelineResult pipelineResult = options.pipeline().run();
          // for unsupported runners (e.g. direct runner), set jobId the same as jobName
          jobId = options.jobName();
          MANAGED_JOBS.put(jobId, pipelineResult);
          // for unsupported runners (e.g. direct runner), return a wrapped LaunchInfo
          return LaunchInfo.builder()
              .setJobId(jobId)
              .setProjectId(project)
              .setRegion(region)
              .setCreateTime(jobProperties.get("createTime"))
              .setSdk("DirectBeam")
              .setVersion("0.0.1")
              .setJobType(jobProperties.get("jobType"))
              .setRunner(options.getParameter("runner"))
              .setParameters(options.parameters())
              .setState(JobState.RUNNING)
              .build();
        }
        break;
      case PYTHON:
        checkState(
            options.executable() != null,
            "Cannot launch a dataflow job "
                + "without executable specified. Please specify executable and try again!");
        if (options.requirementsFile() != null) {
          // install requirements
          cmd.add(
              "virtualenv . && source ./bin/activate && pip3 install -r "
                  + options.requirementsFile());
          cmd.add("&&");
        }
        LOG.info("Using the executable at {}", options.executable());
        cmd.add("python3");
        cmd.add(options.executable());
        cmd.addAll(extractOptions(project, region, options));
        if (options.requirementsFile() != null) {
          cmd.add("&&");
          cmd.add("deactivate");
        }
        jobId = executeCommandAndParseResponse(String.join(" ", cmd));
        break;
      case GO:
        checkState(
            options.executable() != null,
            "Cannot launch a dataflow job "
                + "without executable specified. Please specify executable and try again!");
        LOG.info("Using the executable at {}", options.executable());
        cmd.add("go");
        cmd.add("run");
        cmd.add(options.executable());
        cmd.addAll(extractOptions(project, region, options));
        jobId = executeCommandAndParseResponse(String.join(" ", cmd));
        break;
      default:
        throw new RuntimeException(
            String.format(
                "Invalid sdk %s specified. " + "sdk can be one of java, python, or go.",
                options.sdk()));
    }
    // Wait until the job is active to get more information
    JobState state = waitUntilActive(project, region, jobId);
    Job job = getJob(project, region, jobId, "JOB_VIEW_DESCRIPTION");
    LOG.info("Received Dataflow job {}: {}", job.getId(), formatForLogging(job));

    return getJobInfo(options, state, job);
  }

  @Override
  protected LaunchInfo.Builder getJobInfoBuilder(LaunchConfig options, JobState state, Job job) {
    // get intermediate builder from base class method
    LaunchInfo.Builder builder = super.getJobInfoBuilder(options, state, job);
    // config pipelineName
    String pipelineName = PipelineUtils.extractJobName(options.jobName());
    String overrideName = null;
    if (pipelineName.startsWith("write")) {
      overrideName = System.getProperty(WRITE_PIPELINE_NAME_OVERWRITE);
    } else if (pipelineName.startsWith("read")) {
      overrideName = System.getProperty(READ_PIPELINE_NAME_OVERWRITE);
    }
    if (!Strings.isNullOrEmpty(overrideName)) {
      pipelineName = overrideName;
    }
    builder.setPipelineName(pipelineName);
    return builder;
  }

  private List<String> extractOptions(String project, String region, LaunchConfig options) {
    List<String> additionalOptions = new ArrayList<>();

    // add pipeline options from beamTestPipelineOptions system property to preserve the
    // pipeline options already set in TestPipeline.
    @Nullable
    String beamTestPipelineOptions = System.getProperty(PROPERTY_BEAM_TEST_PIPELINE_OPTIONS);
    if (!Strings.isNullOrEmpty(beamTestPipelineOptions)) {
      try {
        additionalOptions.addAll(MAPPER.readValue(beamTestPipelineOptions, List.class));
      } catch (IOException e) {
        throw new RuntimeException(
            "Unable to instantiate test options from system property "
                + PROPERTY_BEAM_TEST_PIPELINE_OPTIONS
                + ":"
                + System.getProperty(PROPERTY_BEAM_TEST_PIPELINE_OPTIONS),
            e);
      }
    }

    // add pipeline options from options.parameters
    for (Map.Entry<String, String> parameter : options.parameters().entrySet()) {
      additionalOptions.add(String.format("--%s=%s", parameter.getKey(), parameter.getValue()));
    }
    additionalOptions.add(String.format("--project=%s", project));
    additionalOptions.add(String.format("--region=%s", region));
    return additionalOptions;
  }

  /** Executes the specified command and parses the response to get the Job ID. */
  private String executeCommandAndParseResponse(String cmd) throws IOException {
    LOG.info("Running command: {}", cmd);
    Process process =
        new ProcessBuilder().command("/bin/bash", "-c", cmd).redirectErrorStream(true).start();
    String output =
        new String(ByteStreams.toByteArray(process.getInputStream()), StandardCharsets.UTF_8);
    LOG.info(output);
    Matcher m = JOB_ID_PATTERN.matcher(output);
    if (!m.find()) {
      throw new RuntimeException(
          String.format(
              "Dataflow output in unexpected format. Failed to parse Dataflow Job ID. "
                  + "Result from process: %s",
              output));
    }
    String jobId = m.group(1);
    LOG.info("Submitted job: {}", jobId);
    return jobId;
  }

  /** Builder for {@link DefaultPipelineLauncher}. */
  public static final class Builder {
    private Credentials credentials;

    private Builder(Credentials credentials) {
      this.credentials = credentials;
    }

    public Credentials getCredentials() {
      return credentials;
    }

    public DefaultPipelineLauncher build() {
      return new DefaultPipelineLauncher(this);
    }
  }
}
