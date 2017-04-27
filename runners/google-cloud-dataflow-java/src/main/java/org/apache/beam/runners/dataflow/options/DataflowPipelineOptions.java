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

import java.io.IOException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.util.IOChannelUtils;

/**
 * Options that can be used to configure the {@link DataflowRunner}.
 */
@Description("Options that configure the Dataflow pipeline.")
public interface DataflowPipelineOptions
    extends PipelineOptions, GcpOptions, ApplicationNameOptions, DataflowPipelineDebugOptions,
        DataflowPipelineWorkerPoolOptions, BigQueryOptions, GcsOptions, StreamingOptions,
        CloudDebuggerOptions, DataflowWorkerLoggingOptions, DataflowProfilingOptions,
        PubsubOptions {

  @Description("Project id. Required when running a Dataflow in the cloud. "
      + "See https://cloud.google.com/storage/docs/projects for further details.")
  @Override
  @Validation.Required
  @Default.InstanceFactory(DefaultProjectFactory.class)
  String getProject();
  @Override
  void setProject(String value);

  /**
   * GCS path for staging local files, e.g. gs://bucket/object
   *
   * <p>Must be a valid Cloud Storage URL, beginning with the prefix "gs://"
   *
   * <p>If {@link #getStagingLocation()} is not set, it will default to
   * {@link GcpOptions#getGcpTempLocation()}. {@link GcpOptions#getGcpTempLocation()}
   * must be a valid GCS path.
   */
  @Description("GCS path for staging local files, e.g. \"gs://bucket/object\". "
      + "Must be a valid Cloud Storage URL, beginning with the prefix \"gs://\". "
      + "If stagingLocation is unset, defaults to gcpTempLocation with \"/staging\" suffix.")
  @Default.InstanceFactory(StagingLocationFactory.class)
  String getStagingLocation();
  void setStagingLocation(String value);

  /**
   * Whether to update the currently running pipeline with the same name as this one.
   */
  @Description(
      "If set, replace the existing pipeline with the name specified by --jobName with "
          + "this pipeline, preserving state.")
  boolean isUpdate();
  void setUpdate(boolean value);

  /**
   * Where the runner should generate a template file. Must either be local or Cloud Storage.
   */
  @Description("Where the runner should generate a template file. "
      + "Must either be local or Cloud Storage.")
  String getTemplateLocation();
  void setTemplateLocation(String value);

  /**
   * Run the job as a specific service account, instead of the default GCE robot.
   */
  @Hidden
  @Experimental
  @Description(
      "Run the job as a specific service account, instead of the default GCE robot.")
  String getServiceAccount();
  void setServiceAccount(String value);

  /**
   * The Google Compute Engine
   * <a href="https://cloud.google.com/compute/docs/regions-zones/regions-zones">region</a>
   * for creating Dataflow jobs.
   *
   * <p>NOTE: The Cloud Dataflow service does not yet honor this setting. However, once service
   * support is added then users of this SDK will be able to control the region.
   */
  @Hidden
  @Experimental
  @Description("The Google Compute Engine region for creating Dataflow jobs. See "
      + "https://cloud.google.com/compute/docs/regions-zones/regions-zones for a list of valid "
      + "options. Default is up to the Dataflow service.")
  @Default.String("us-central1")
  String getRegion();
  void setRegion(String region);

  /**
   * Returns a default staging location under {@link GcpOptions#getGcpTempLocation}.
   */
  class StagingLocationFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      String gcpTempLocation;
      try {
        gcpTempLocation = gcsOptions.getGcpTempLocation();
      } catch (Exception e) {
        throw new IllegalArgumentException(
        "Error constructing default value for stagingLocation: failed to retrieve gcpTempLocation. "
            + "Either stagingLocation must be set explicitly or a valid value must be provided"
            + "for gcpTempLocation.", e);
      }
      try {
        gcsOptions.getPathValidator().validateOutputFilePrefixSupported(gcpTempLocation);
      } catch (Exception e) {
        throw new IllegalArgumentException(String.format(
            "Error constructing default value for stagingLocation: gcpTempLocation is not"
            + " a valid GCS path, %s. ", gcpTempLocation), e);
      }
      try {
        return IOChannelUtils.resolve(gcpTempLocation, "staging");
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format(
            "Unable to resolve stagingLocation from gcpTempLocation: %s."
            + " Please set the staging location explicitly.", gcpTempLocation), e);
      }
    }
  }
}
