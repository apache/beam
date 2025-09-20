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

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.FileStagingOptions;
import org.apache.beam.sdk.options.Hidden;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Options that are used to configure the Dataflow pipeline worker pool. */
@Description("Options that are used to configure the Dataflow pipeline worker pool.")
public interface DataflowPipelineWorkerPoolOptions extends GcpOptions, FileStagingOptions {
  /**
   * Number of workers to use when executing the Dataflow job. Note that selection of an autoscaling
   * algorithm other then {@code NONE} will affect the size of the worker pool. If left unspecified,
   * the Dataflow service will determine the number of workers.
   */
  @Description(
      "Number of workers to use when executing the Dataflow job. Note that "
          + "selection of an autoscaling algorithm other then \"NONE\" will affect the "
          + "size of the worker pool. If left unspecified, the Dataflow service will "
          + "determine the number of workers.")
  int getNumWorkers();

  void setNumWorkers(int value);

  /** Type of autoscaling algorithm to use. */
  enum AutoscalingAlgorithmType {
    /** Use numWorkers machines. Do not autoscale the worker pool. */
    NONE("AUTOSCALING_ALGORITHM_NONE"),

    /** @deprecated use {@link #THROUGHPUT_BASED}. */
    @Deprecated
    BASIC("AUTOSCALING_ALGORITHM_BASIC"),

    /** Autoscale the workerpool based on throughput (up to maxNumWorkers). */
    THROUGHPUT_BASED("AUTOSCALING_ALGORITHM_BASIC");

    private final String algorithm;

    private AutoscalingAlgorithmType(String algorithm) {
      this.algorithm = algorithm;
    }

    /** Returns the string representation of this type. */
    public String getAlgorithm() {
      return this.algorithm;
    }
  }

  /**
   * The autoscaling algorithm to use for the workerpool.
   *
   * <ul>
   *   <li>NONE: does not change the size of the worker pool.
   *   <li>BASIC: autoscale the worker pool size up to maxNumWorkers until the job completes.
   *   <li>THROUGHPUT_BASED: autoscale the workerpool based on throughput (up to maxNumWorkers).
   * </ul>
   */
  @Description(
      "The autoscaling algorithm to use for the workerpool. "
          + "NONE: does not change the size of the worker pool. "
          + "BASIC (deprecated): autoscale the worker pool size up to maxNumWorkers until the job "
          + "completes. "
          + "THROUGHPUT_BASED: autoscale the workerpool based on throughput (up to maxNumWorkers).")
  AutoscalingAlgorithmType getAutoscalingAlgorithm();

  void setAutoscalingAlgorithm(AutoscalingAlgorithmType value);

  /**
   * The maximum number of workers to use for the workerpool. This options limits the size of the
   * workerpool for the lifetime of the job, including <a
   * href="https://cloud.google.com/dataflow/pipelines/updating-a-pipeline">pipeline updates</a>. If
   * left unspecified, the Dataflow service will compute a ceiling.
   */
  @Description(
      "The maximum number of workers to use for the workerpool. This options limits the "
          + "size of the workerpool for the lifetime of the job, including pipeline updates. "
          + "If left unspecified, the Dataflow service will compute a ceiling.")
  int getMaxNumWorkers();

  void setMaxNumWorkers(int value);

  /** Remote worker disk size, in gigabytes, or 0 to use the default size. */
  @Description("Remote worker disk size, in gigabytes, or 0 to use the default size.")
  int getDiskSizeGb();

  void setDiskSizeGb(int value);

  /** Container image used as Dataflow worker harness image. */
  /** @deprecated Use {@link #getSdkContainerImage} instead. */
  @Description(
      "Container image used to configure a Dataflow worker. "
          + "Can only be used for official Dataflow container images. "
          + "Prefer using sdkContainerImage instead.")
  @Deprecated
  @Hidden
  String getWorkerHarnessContainerImage();

  /** @deprecated Use {@link #setSdkContainerImage} instead. */
  @Deprecated
  @Hidden
  void setWorkerHarnessContainerImage(String value);

  /**
   * Container image used to configure SDK execution environment on worker. Used for custom
   * containers on portable pipelines only.
   */
  @Description(
      "Container image used to configure the SDK execution environment of "
          + "pipeline code on a worker. For non-portable pipelines, can only be "
          + "used for official Dataflow container images.")
  String getSdkContainerImage();

  void setSdkContainerImage(String value);

  /**
   * GCE <a href="https://cloud.google.com/compute/docs/networking">network</a> for launching
   * workers.
   *
   * <p>Default is up to the Dataflow service.
   */
  @Description(
      "GCE network for launching workers. For more information, see the reference "
          + "documentation https://cloud.google.com/compute/docs/networking. "
          + "Default is up to the Dataflow service.")
  String getNetwork();

  void setNetwork(String value);

  /**
   * GCE <a href="https://cloud.google.com/compute/docs/networking">subnetwork</a> for launching
   * workers.
   *
   * <p>Default is up to the Dataflow service. Expected format is
   * regions/REGION/subnetworks/SUBNETWORK or the fully qualified subnetwork name, beginning with
   * https://..., e.g. https://www.googleapis.com/compute/alpha/projects/PROJECT/
   * regions/REGION/subnetworks/SUBNETWORK
   */
  @Description(
      "GCE subnetwork for launching workers. For more information, see the reference "
          + "documentation https://cloud.google.com/compute/docs/networking. "
          + "Default is up to the Dataflow service.")
  String getSubnetwork();

  void setSubnetwork(String value);

  /**
   * Machine type to create Dataflow worker VMs as.
   *
   * <p>See <a href="https://cloud.google.com/compute/docs/machine-types">GCE machine types</a> for
   * a list of valid options.
   *
   * <p>If unset, the Dataflow service will choose a reasonable default.
   */
  @Description(
      "Machine type to create Dataflow worker VMs as. See "
          + "https://cloud.google.com/compute/docs/machine-types for a list of valid options. "
          + "If unset, the Dataflow service will choose a reasonable default.")
  String getWorkerMachineType();

  void setWorkerMachineType(String value);

  /**
   * Specifies what type of persistent disk is used. The value is a full disk type resource, e.g.,
   * compute.googleapis.com/projects//zones//diskTypes/pd-ssd. For more information, see the <a
   * href="https://cloud.google.com/compute/docs/reference/latest/diskTypes">API reference
   * documentation for DiskTypes</a>.
   */
  @Description(
      "Specifies what type of persistent disk is used. The "
          + "value is a full URL of a disk type resource, e.g., "
          + "compute.googleapis.com/projects//zones//diskTypes/pd-ssd. For more "
          + "information, see the API reference documentation for DiskTypes: "
          + "https://cloud.google.com/compute/docs/reference/latest/diskTypes")
  String getWorkerDiskType();

  void setWorkerDiskType(String value);

  /**
   * Specifies whether worker pools should be started with public IP addresses.
   *
   * <p>WARNING: This feature is available only through allowlist.
   */
  @Description(
      "Specifies whether worker pools should be started with public IP addresses. WARNING:"
          + "This feature is available only through allowlist.")
  @Nullable
  Boolean getUsePublicIps();

  void setUsePublicIps(@Nullable Boolean value);

  /**
   * Specifies a Minimum CPU platform for VM instances.
   *
   * <p>More details see <a
   * href='https://cloud.google.com/compute/docs/instances/specify-min-cpu-platform'>Specifying
   * Pipeline Execution Parameters</a>.
   */
  @Description("GCE minimum CPU platform. Default is determined by GCP.")
  @Nullable
  String getMinCpuPlatform();

  void setMinCpuPlatform(String minCpuPlatform);
}
