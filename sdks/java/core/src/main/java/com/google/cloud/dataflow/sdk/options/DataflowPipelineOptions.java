/*
 * Copyright (C) 2015 Google Inc.
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

import com.google.cloud.dataflow.sdk.runners.DataflowPipeline;
import com.google.common.base.MoreObjects;

import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Options that can be used to configure the {@link DataflowPipeline}.
 */
@Description("Options that configure the Dataflow pipeline.")
public interface DataflowPipelineOptions extends
    PipelineOptions, GcpOptions, ApplicationNameOptions, DataflowPipelineDebugOptions,
    DataflowPipelineWorkerPoolOptions, BigQueryOptions,
    GcsOptions, StreamingOptions, CloudDebuggerOptions, DataflowWorkerLoggingOptions,
    DataflowProfilingOptions {

  static final String DATAFLOW_STORAGE_LOCATION = "Dataflow Storage Location";

  @Description("Project id. Required when running a Dataflow in the cloud. "
      + "See https://cloud.google.com/storage/docs/projects for further details.")
  @Override
  @Validation.Required
  @Default.InstanceFactory(DefaultProjectFactory.class)
  String getProject();
  @Override
  void setProject(String value);

  /**
   * GCS path for temporary files, e.g. gs://bucket/object
   *
   * <p>Must be a valid Cloud Storage URL, beginning with the prefix "gs://"
   *
   * <p>At least one of {@link #getTempLocation()} or {@link #getStagingLocation()} must be set. If
   * {@link #getTempLocation()} is not set, then the Dataflow pipeline defaults to using
   * {@link #getStagingLocation()}.
   */
  @Description("GCS path for temporary files, eg \"gs://bucket/object\". "
      + "Must be a valid Cloud Storage URL, beginning with the prefix \"gs://\". "
      + "At least one of tempLocation or stagingLocation must be set. If tempLocation is unset, "
      + "defaults to using stagingLocation.")
  @Validation.Required(groups = {DATAFLOW_STORAGE_LOCATION})
  String getTempLocation();
  void setTempLocation(String value);

  /**
   * GCS path for staging local files, e.g. gs://bucket/object
   *
   * <p>Must be a valid Cloud Storage URL, beginning with the prefix "gs://"
   *
   * <p>At least one of {@link #getTempLocation()} or {@link #getStagingLocation()} must be set. If
   * {@link #getTempLocation()} is not set, then the Dataflow pipeline defaults to using
   * {@link #getStagingLocation()}.
   */
  @Description("GCS path for staging local files, e.g. \"gs://bucket/object\". "
      + "Must be a valid Cloud Storage URL, beginning with the prefix \"gs://\". "
      + "At least one of stagingLocation or tempLocation must be set. If stagingLocation is unset, "
      + "defaults to using tempLocation.")
  @Validation.Required(groups = {DATAFLOW_STORAGE_LOCATION})
  String getStagingLocation();
  void setStagingLocation(String value);

  /**
   * The Dataflow job name is used as an idempotence key within the Dataflow service.
   * If there is an existing job that is currently active, another active job with the same
   * name will not be able to be created. Defaults to using the ApplicationName-UserName-Date.
   */
  @Description("The Dataflow job name is used as an idempotence key within the Dataflow service. "
      + "If there is an existing job that is currently active, another active job with the same "
      + "name will not be able to be created. Defaults to using the ApplicationName-UserName-Date.")
  @Default.InstanceFactory(JobNameFactory.class)
  String getJobName();
  void setJobName(String value);

  /**
   * Whether to update the currently running pipeline with the same name as this one.
   */
  @Override
  @SuppressWarnings("deprecation") // base class member deprecated in favor of this one.
  @Description(
      "If set, replace the existing pipeline with the name specified by --jobName with "
          + "this pipeline, preserving state.")
  boolean getUpdate();
  @Override
  @SuppressWarnings("deprecation") // base class member deprecated in favor of this one.
  void setUpdate(boolean value);

  /**
   * Returns a normalized job name constructed from {@link ApplicationNameOptions#getAppName()}, the
   * local system user name (if available), and the current time. The normalization makes sure that
   * the job name matches the required pattern of [a-z]([-a-z0-9]*[a-z0-9])? and length limit of 40
   * characters.
   *
   * <p>This job name factory is only able to generate one unique name per second per application
   * and user combination.
   */
  public static class JobNameFactory implements DefaultValueFactory<String> {
    private static final DateTimeFormatter FORMATTER =
        DateTimeFormat.forPattern("MMddHHmmss").withZone(DateTimeZone.UTC);

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
      return normalizedAppName + "-" + normalizedUserName + "-" + datePart;
    }
  }
}
