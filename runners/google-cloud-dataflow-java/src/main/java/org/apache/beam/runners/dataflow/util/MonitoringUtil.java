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
package org.apache.beam.runners.dataflow.util;

import static org.apache.beam.runners.dataflow.util.TimeUtil.fromCloudTime;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.ListJobMessagesResponse;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowClient;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper class for monitoring jobs submitted to the service. */
public class MonitoringUtil {

  private static final String GCLOUD_DATAFLOW_PREFIX = "gcloud dataflow";
  private static final String ENDPOINT_OVERRIDE_ENV_VAR =
      "CLOUDSDK_API_ENDPOINT_OVERRIDES_DATAFLOW";

  private static final Map<String, State> DATAFLOW_STATE_TO_JOB_STATE =
      ImmutableMap.<String, State>builder()
          .put("JOB_STATE_UNKNOWN", State.UNKNOWN)
          .put("JOB_STATE_STOPPED", State.STOPPED)
          .put("JOB_STATE_RUNNING", State.RUNNING)
          .put("JOB_STATE_DONE", State.DONE)
          .put("JOB_STATE_FAILED", State.FAILED)
          .put("JOB_STATE_CANCELLED", State.CANCELLED)
          .put("JOB_STATE_UPDATED", State.UPDATED)
          // A DRAINING job is still running - the closest mapping is RUNNING.
          .put("JOB_STATE_DRAINING", State.RUNNING)
          // A DRAINED job has successfully terminated - the closest mapping is DONE.
          .put("JOB_STATE_DRAINED", State.DONE)
          .build();
  private static final String JOB_MESSAGE_ERROR = "JOB_MESSAGE_ERROR";
  private static final String JOB_MESSAGE_WARNING = "JOB_MESSAGE_WARNING";
  private static final String JOB_MESSAGE_BASIC = "JOB_MESSAGE_BASIC";
  private static final String JOB_MESSAGE_DETAILED = "JOB_MESSAGE_DETAILED";
  private static final String JOB_MESSAGE_DEBUG = "JOB_MESSAGE_DEBUG";

  private final DataflowClient dataflowClient;

  /**
   * An interface that can be used for defining callbacks to receive a list of JobMessages
   * containing monitoring information.
   */
  public interface JobMessagesHandler {
    /** Process the rows. */
    void process(List<JobMessage> messages);
  }

  /** A handler that logs monitoring messages. */
  public static class LoggingHandler implements JobMessagesHandler {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingHandler.class);

    @Override
    public void process(List<JobMessage> messages) {
      for (JobMessage message : messages) {
        if (Strings.isNullOrEmpty(message.getMessageText())) {
          continue;
        }

        @Nullable Instant time = fromCloudTime(message.getTime());
        String logMessage =
            (time == null ? "UNKNOWN TIMESTAMP: " : time + ": ") + message.getMessageText();
        switch (message.getMessageImportance()) {
          case JOB_MESSAGE_ERROR:
            LOG.error(logMessage);
            break;
          case JOB_MESSAGE_WARNING:
            LOG.warn(logMessage);
            break;
          case JOB_MESSAGE_BASIC:
          case JOB_MESSAGE_DETAILED:
            LOG.info(logMessage);
            break;
          case JOB_MESSAGE_DEBUG:
            LOG.debug(logMessage);
            break;
          default:
            LOG.trace(logMessage);
        }
      }
    }
  }

  /** Construct a helper for monitoring. */
  public MonitoringUtil(DataflowClient dataflowClient) {
    this.dataflowClient = dataflowClient;
  }

  /** Comparator for sorting rows in increasing order based on timestamp. */
  public static class TimeStampComparator implements Comparator<JobMessage>, Serializable {
    @Override
    public int compare(JobMessage o1, JobMessage o2) {
      @Nullable Instant t1 = fromCloudTime(o1.getTime());
      if (t1 == null) {
        return -1;
      }
      @Nullable Instant t2 = fromCloudTime(o2.getTime());
      if (t2 == null) {
        return 1;
      }
      return t1.compareTo(t2);
    }
  }

  /**
   * Return job messages sorted in ascending order by timestamp.
   *
   * @param jobId The id of the job to get the messages for.
   * @param startTimestampMs Return only those messages with a timestamp greater than this value.
   * @return collection of messages
   */
  public List<JobMessage> getJobMessages(String jobId, long startTimestampMs) throws IOException {
    // TODO: Allow filtering messages by importance
    Instant startTimestamp = new Instant(startTimestampMs);
    ArrayList<JobMessage> allMessages = new ArrayList<>();
    String pageToken = null;
    while (true) {
      ListJobMessagesResponse response = dataflowClient.listJobMessages(jobId, pageToken);
      if (response == null || response.getJobMessages() == null) {
        return allMessages;
      }

      for (JobMessage m : response.getJobMessages()) {
        @Nullable Instant timestamp = fromCloudTime(m.getTime());
        if (timestamp == null) {
          continue;
        }
        if (timestamp.isAfter(startTimestamp)) {
          allMessages.add(m);
        }
      }

      if (response.getNextPageToken() == null) {
        break;
      } else {
        pageToken = response.getNextPageToken();
      }
    }

    allMessages.sort(new TimeStampComparator());
    return allMessages;
  }

  /**
   * @deprecated this method defaults the region to "us-central1". Prefer using the overload with an
   *     explicit regionId parameter.
   */
  @Deprecated
  public static String getJobMonitoringPageURL(String projectName, String jobId) {
    return getJobMonitoringPageURL(projectName, "us-central1", jobId);
  }

  public static String getJobMonitoringPageURL(String projectName, String regionId, String jobId) {
    try {
      // Project name is allowed in place of the project id: the user will be redirected to a URL
      // that has the project name replaced with project id.
      return String.format(
          "https://console.cloud.google.com/dataflow/jobs/%s/%s?project=%s",
          URLEncoder.encode(regionId, StandardCharsets.UTF_8.name()),
          URLEncoder.encode(jobId, StandardCharsets.UTF_8.name()),
          URLEncoder.encode(projectName, StandardCharsets.UTF_8.name()));
    } catch (UnsupportedEncodingException e) {
      // Should never happen.
      throw new AssertionError("UTF-8 encoding is not supported by the environment", e);
    }
  }

  public static String getGcloudCancelCommand(DataflowPipelineOptions options, String jobId) {

    // If using a different Dataflow API than default, prefix command with an API override.
    String dataflowApiOverridePrefix = "";
    String apiUrl = options.getDataflowClient().getBaseUrl();
    if (!apiUrl.equals(Dataflow.DEFAULT_BASE_URL)) {
      dataflowApiOverridePrefix = String.format("%s=%s ", ENDPOINT_OVERRIDE_ENV_VAR, apiUrl);
    }

    // Assemble cancel command from optional prefix and project/job parameters.
    return String.format(
        "%s%s jobs --project=%s cancel --region=%s %s",
        dataflowApiOverridePrefix,
        GCLOUD_DATAFLOW_PREFIX,
        options.getProject(),
        options.getRegion(),
        jobId);
  }

  public static State toState(String stateName) {
    return MoreObjects.firstNonNull(DATAFLOW_STATE_TO_JOB_STATE.get(stateName), State.UNRECOGNIZED);
  }
}
