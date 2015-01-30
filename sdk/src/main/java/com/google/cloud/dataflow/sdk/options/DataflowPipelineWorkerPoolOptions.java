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

import java.util.List;

/**
 * Options which are used to configure the Dataflow pipeline worker pool.
 */
public interface DataflowPipelineWorkerPoolOptions extends PipelineOptions {
  /**
   * Disk source image to use by VMs for jobs.
   * @see <a href="https://developers.google.com/compute/docs/images">Compute Engine Images</a>
   */
  @Description("Dataflow VM disk image.")
  String getDiskSourceImage();
  void setDiskSourceImage(String value);

  /**
   * Number of workers to use in remote execution.
   */
  @Description("Number of workers, when using remote execution")
  @Default.Integer(3)
  int getNumWorkers();
  void setNumWorkers(int value);

  /**
   * Type of autoscaling algorithm to use. These types are experimental and subject to change.
   */
  public enum AutoscalingAlgorithmType {
    /** Use numWorkers machines. Do not autoscale the worker pool. */
    NONE("AUTOSCALING_ALGORITHM_NONE"),

    /** Autoscale the workerpool size up to maxNumWorkers until the job completes. */
    BASIC("AUTOSCALING_ALGORITHM_BASIC");

    private final String algorithm;

    private AutoscalingAlgorithmType(String algorithm) {
      this.algorithm = algorithm;
    }

    /** Returns the string representation of this type. */
    public String getAlgorithm() {
      return this.algorithm;
    }
  }

  @Description("(experimental) The autoscaling algorithm to use for the workerpool.")
  @Default.Enum("NONE")
  AutoscalingAlgorithmType getAutoscalingAlgorithm();
  void setAutoscalingAlgorithm(AutoscalingAlgorithmType value);

  /**
   * Max number of workers to use when using workerpool autoscaling.
   * This option is experimental and subject to change.
   */
  @Description("Max number of workers to use, when using autoscaling")
  @Default.Integer(20)
  int getMaxNumWorkers();
  void setMaxNumWorkers(int value);

  /**
   * Remote worker disk size, in gigabytes, or 0 to use the default size.
   */
  @Description("Remote worker disk size, in gigabytes, or 0 to use the default size.")
  int getDiskSizeGb();
  void setDiskSizeGb(int value);

  /**
   * GCE <a href="https://developers.google.com/compute/docs/zones"
   * >availability zone</a> for launching workers.
   *
   * <p> Default is up to the service.
   */
  @Description("GCE availability zone for launching workers. "
      + "Default is up to the service")
  String getZone();
  void setZone(String value);

  /**
   * Type of API for handling cluster management,i.e. resizing, healthchecking, etc.
   */
  public enum ClusterManagerApiType {
    COMPUTE_ENGINE("compute.googleapis.com"),
    REPLICA_POOL("replicapool.googleapis.com");

    private final String apiServiceName;

    private ClusterManagerApiType(String apiServiceName) {
      this.apiServiceName = apiServiceName;
    }

    public String getApiServiceName() {
      return this.apiServiceName;
    }
  }

  @Description("Type of API for handling cluster management,i.e. resizing, healthchecking, etc.")
  @Default.Enum("COMPUTE_ENGINE")
  ClusterManagerApiType getClusterManagerApi();
  void setClusterManagerApi(ClusterManagerApiType value);

  /**
   * Machine type to create worker VMs as.
   */
  @Description("Dataflow VM machine type for workers.")
  String getWorkerMachineType();
  void setWorkerMachineType(String value);

  /**
   * The policy for tearing down the workers spun up by the service.
   */
  public enum TeardownPolicy {
    TEARDOWN_ALWAYS("TEARDOWN_ALWAYS"),
    TEARDOWN_NEVER("TEARDOWN_NEVER");

    private final String teardownPolicy;

    private TeardownPolicy(String teardownPolicy) {
      this.teardownPolicy = teardownPolicy;
    }

    public String getTeardownPolicyName() {
      return this.teardownPolicy;
    }
  }

  /**
   * Teardown policy for the VMs.
   *
   * <p> By default this is left unset and the service sets the default policy.
   */
  @Description("The teardown policy for the VMs.")
  TeardownPolicy getTeardownPolicy();
  void setTeardownPolicy(TeardownPolicy value);

  /**
   * List of local files to make available to workers.
   * <p>
   * Jars are placed on the worker's classpath.
   * <p>
   * The default value is the list of jars from the main program's classpath.
   */
  @Description("Files to stage on GCS and make available to "
      + "workers.  The default value is all files from the classpath.")
  List<String> getFilesToStage();
  void setFilesToStage(List<String> value);
}
