/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.options;

import com.google.api.services.dataflow.Dataflow;
import com.google.cloud.dataflow.sdk.runners.DataflowPipeline;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.common.base.MoreObjects;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Options which can be used to configure the {@link DataflowPipeline}.
 */
public interface DataflowPipelineOptions extends
    PipelineOptions, GcpOptions, ApplicationNameOptions, DataflowPipelineDebugOptions,
    DataflowPipelineWorkerPoolOptions, BigQueryOptions,
    GcsOptions, StreamingOptions, CloudDebuggerOptions {

  /**
   * GCS path for temporary files.
   * <p>
   * Must be a valid Cloud Storage url, beginning with the prefix "gs://"
   * <p>
   * At least one of {@link #getTempLocation()} or {@link #getStagingLocation()} must be set. If
   * {@link #getTempLocation()} is not set, then the Dataflow pipeline defaults to using
   * {@link #getStagingLocation()}.
   */
  @Description("GCS path for temporary files, eg \"gs://bucket/object\".  "
      + "Defaults to stagingLocation.")
  String getTempLocation();
  void setTempLocation(String value);

  /**
   * GCS path for staging local files.
   * <p>
   * If {@link #getStagingLocation()} is not set, then the Dataflow pipeline defaults to a staging
   * directory within {@link #getTempLocation}.
   * <p>
   * At least one of {@link #getTempLocation()} or {@link #getStagingLocation()} must be set.
   */
  @Description("GCS staging path.  Defaults to a staging directory"
      + " with the tempLocation")
  String getStagingLocation();
  void setStagingLocation(String value);

  /**
   * The job name is used as an idempotence key within the Dataflow service. If there
   * is an existing job which is currently active, another job with the same name will
   * not be able to be created.
   */
  @Description("Dataflow job name, to uniquely identify active jobs. "
      + "Defaults to using the ApplicationName-UserName-Date.")
  @Default.InstanceFactory(JobNameFactory.class)
  String getJobName();
  void setJobName(String value);

  /**
   * Returns a normalized job name constructed from {@link ApplicationNameOptions#getAppName()}, the
   * local system user name (if available), and the current time. The normalization makes sure that
   * the job name matches the required pattern of [a-z]([-a-z0-9]*[a-z0-9])? and length limit of 40
   * characters.
   * <p>
   * This job name factory is only able to generate one unique name per second per application and
   * user combination.
   */
  public static class JobNameFactory implements DefaultValueFactory<String> {
    private static final DateTimeFormatter FORMATTER =
        DateTimeFormat.forPattern("MMddHHmmss").withZone(DateTimeZone.UTC);
    private static final int MAX_APP_NAME = 19;
    private static final int MAX_USER_NAME = 9;

    @Override
    public String create(PipelineOptions options) {
      String appName = options.as(ApplicationNameOptions.class).getAppName();
      String normalizedAppName = appName == null || appName.length() == 0 ? "dataflow"
          : appName.toLowerCase()
                   .replaceAll("[^a-z0-9]", "0")
                   .replaceAll("^[^a-z]", "a");
      String userName = MoreObjects.firstNonNull(System.getProperty("user.name"), "");
      String normalizedUserName = userName.toLowerCase()
                                          .replaceAll("[^a-z0-9]", "0");
      String datePart = FORMATTER.print(DateTimeUtils.currentTimeMillis());

      // Maximize the amount of the app name and user name we can use.
      normalizedAppName = normalizedAppName.substring(0,
            Math.min(normalizedAppName.length(),
                MAX_APP_NAME + Math.max(0, MAX_USER_NAME - normalizedUserName.length())));
      normalizedUserName = normalizedUserName.substring(0,
            Math.min(userName.length(),
                MAX_USER_NAME + Math.max(0, MAX_APP_NAME - normalizedAppName.length())));
      return normalizedAppName + "-" + normalizedUserName + "-" + datePart;
    }
  }

  /** Alternative Dataflow client. */
  @JsonIgnore
  @Default.InstanceFactory(DataflowClientFactory.class)
  Dataflow getDataflowClient();
  void setDataflowClient(Dataflow value);

  /** Returns the default Dataflow client built from the passed in PipelineOptions. */
  public static class DataflowClientFactory implements DefaultValueFactory<Dataflow> {
    @Override
    public Dataflow create(PipelineOptions options) {
        return Transport.newDataflowClient(options.as(DataflowPipelineOptions.class)).build();
    }
  }
}
