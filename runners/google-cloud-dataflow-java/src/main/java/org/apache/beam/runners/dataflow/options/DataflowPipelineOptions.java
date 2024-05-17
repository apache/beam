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

import com.google.api.services.dataflow.Dataflow;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Options that can be used to configure the {@link DataflowRunner}. */
@Description("Options that configure the Dataflow pipeline.")
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public interface DataflowPipelineOptions
    extends PipelineOptions,
        GcpOptions,
        ApplicationNameOptions,
        DataflowPipelineDebugOptions,
        DataflowPipelineWorkerPoolOptions,
        BigQueryOptions,
        GcsOptions,
        StreamingOptions,
        DataflowWorkerLoggingOptions,
        DataflowStreamingPipelineOptions,
        DataflowProfilingOptions,
        PubsubOptions {

  @Description(
      "Project id. Required when running a Dataflow in the cloud. "
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
   * <p>If {@link #getStagingLocation()} is not set, it will default to {@link
   * GcpOptions#getGcpTempLocation()}. {@link GcpOptions#getGcpTempLocation()} must be a valid GCS
   * path.
   */
  @Description(
      "GCS path for staging local files, e.g. \"gs://bucket/object\". "
          + "Must be a valid Cloud Storage URL, beginning with the prefix \"gs://\". "
          + "If stagingLocation is unset, defaults to gcpTempLocation with \"/staging\" suffix.")
  @Default.InstanceFactory(StagingLocationFactory.class)
  String getStagingLocation();

  void setStagingLocation(String value);

  /** Whether to update the currently running pipeline with the same name as this one. */
  @Description(
      "If set, replace the existing pipeline with the name specified by --jobName with "
          + "this pipeline, preserving state.")
  boolean isUpdate();

  void setUpdate(boolean value);

  /** If set, the snapshot from which the job should be created. */
  @Description("If set, the snapshot from which the job should be created.")
  String getCreateFromSnapshot();

  void setCreateFromSnapshot(String value);

  /** Where the runner should generate a template file. Must either be local or Cloud Storage. */
  @Description(
      "Where the runner should generate a template file. "
          + "Must either be local or Cloud Storage.")
  String getTemplateLocation();

  void setTemplateLocation(String value);

  /**
   * Service options are set by the user and configure the service. This decouples service side
   * feature availability from the Apache Beam release cycle.
   */
  @Description(
      "Service options are set by the user and configure the service. This "
          + "decouples service side feature availability from the Apache Beam release cycle. "
          + "For a list of service options, see "
          + "https://cloud.google.com/dataflow/docs/reference/service-options "
          + "in the Dataflow documentation.")
  List<String> getDataflowServiceOptions();

  void setDataflowServiceOptions(List<String> options);

  /** Run the job as a specific service account, instead of the default GCE robot. */
  @Description("Run the job as a specific service account, instead of the default GCE robot.")
  String getServiceAccount();

  void setServiceAccount(String value);

  /**
   * The Google Compute Engine <a
   * href="https://cloud.google.com/compute/docs/regions-zones/regions-zones">region</a> for
   * creating Dataflow jobs.
   */
  @Description(
      "The Google Compute Engine region for creating Dataflow jobs. See "
          + "https://cloud.google.com/compute/docs/regions-zones/regions-zones for a list of valid "
          + "options.")
  @Default.InstanceFactory(DefaultGcpRegionFactory.class)
  String getRegion();

  void setRegion(String region);

  /**
   * Dataflow endpoint to use.
   *
   * <p>Defaults to the current version of the Google Cloud Dataflow API, at the time the current
   * SDK version was released.
   *
   * <p>If the string contains "://", then this is treated as a URL, otherwise {@link
   * #getApiRootUrl()} is used as the root URL.
   */
  @Description(
      "The URL for the Dataflow API. If the string contains \"://\", this"
          + " will be treated as the entire URL, otherwise will be treated relative to apiRootUrl.")
  @Override
  @Default.String(Dataflow.DEFAULT_SERVICE_PATH)
  String getDataflowEndpoint();

  @Override
  void setDataflowEndpoint(String value);

  /** Labels that will be applied to the billing records for this job. */
  @Description("Labels that will be applied to the billing records for this job.")
  Map<String, String> getLabels();

  void setLabels(Map<String, String> labels);

  /** The URL of the staged portable pipeline. */
  @Description("The URL of the staged portable pipeline")
  String getPipelineUrl();

  void setPipelineUrl(String urlString);

  @Description("The customized dataflow worker jar")
  String getDataflowWorkerJar();

  void setDataflowWorkerJar(String dataflowWorkerJar);

  /** Set of available Flexible Resource Scheduling goals. */
  enum FlexResourceSchedulingGoal {
    /** No goal specified. */
    UNSPECIFIED,

    /** Optimize for lower execution time. */
    SPEED_OPTIMIZED,

    /** Optimize for lower cost. */
    COST_OPTIMIZED,
  }

  /** This option controls Flexible Resource Scheduling mode. */
  @Description("Controls the Flexible Resource Scheduling mode.")
  @Default.Enum("UNSPECIFIED")
  FlexResourceSchedulingGoal getFlexRSGoal();

  void setFlexRSGoal(FlexResourceSchedulingGoal goal);

  /** Returns a default staging location under {@link GcpOptions#getGcpTempLocation}. */
  class StagingLocationFactory implements DefaultValueFactory<String> {
    private static final Logger LOG = LoggerFactory.getLogger(StagingLocationFactory.class);

    @Override
    public String create(PipelineOptions options) {
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      LOG.info("No stagingLocation provided, falling back to gcpTempLocation");
      String gcpTempLocation;
      try {
        gcpTempLocation = gcsOptions.getGcpTempLocation();
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Error constructing default value for stagingLocation: failed to retrieve gcpTempLocation. "
                + "Either stagingLocation must be set explicitly or a valid value must be provided "
                + "for gcpTempLocation.",
            e);
      }
      try {
        gcsOptions.getPathValidator().validateOutputFilePrefixSupported(gcpTempLocation);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "Error constructing default value for stagingLocation: gcpTempLocation is not"
                    + " a valid GCS path, %s. ",
                gcpTempLocation),
            e);
      }
      return FileSystems.matchNewResource(gcpTempLocation, true /* isDirectory */)
          .resolve("staging", StandardResolveOptions.RESOLVE_DIRECTORY)
          .toString();
    }
  }

  /** If enabled then the literal key will be logged to Cloud Logging if a hot key is detected. */
  @Description(
      "If enabled then the literal key will be logged to Cloud Logging if a hot key is detected.")
  boolean isHotKeyLoggingEnabled();

  void setHotKeyLoggingEnabled(boolean value);

  /**
   * Open modules needed for reflection that access JDK internals with Java 9+
   *
   * <p>With JDK 16+, <a href="#{https://openjdk.java.net/jeps/403}">JDK internals are strongly
   * encapsulated</a> and can result in an InaccessibleObjectException being thrown if a tool or
   * library uses reflection that access JDK internals. If you see these errors in your worker logs,
   * you can pass in modules to open using the format module/package=target-module(,target-module)*
   * to allow access to the library. E.g. java.base/java.lang=jamm
   *
   * <p>You may see warnings that jamm, a library used to more accurately size objects, is unable to
   * make a private field accessible. To resolve the warning, open the specified module/package to
   * jamm.
   */
  @Description("Open modules needed for reflection with Java 17+.")
  List<String> getJdkAddOpenModules();

  void setJdkAddOpenModules(List<String> options);
}
