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

import com.google.cloud.dataflow.sdk.annotations.Experimental;

import java.util.List;

/**
 * Options that are used to configure the Dataflow pipeline worker pool.
 */
@Description("Options that are used to configure the Dataflow pipeline worker pool.")
public interface DataflowPipelineWorkerPoolOptions extends PipelineOptions {
  /**
   * Disk source image to use by VMs for jobs.
   * @see <a href="https://developers.google.com/compute/docs/images">Compute Engine Images</a>
   */
  @Description("Disk source image to use by VMs for jobs. See "
      + "https://developers.google.com/compute/docs/images for further details.")
  String getDiskSourceImage();
  void setDiskSourceImage(String value);

  /**
   * Number of workers to use when executing the Dataflow job.
   */
  @Description("Number of workers to use when executing the Dataflow job. Note that "
      + "selection of an autoscaling algorithm other then \"NONE\" will affect the "
      + "size of the worker pool.")
  @Default.Integer(3)
  int getNumWorkers();
  void setNumWorkers(int value);

  /**
   * Type of autoscaling algorithm to use.
   */
  @Experimental(Experimental.Kind.AUTOSCALING)
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

  @Description("[Experimental] The autoscaling algorithm to use for the workerpool. "
      + "NONE: does not change the size of the worker pool. "
      + "BASIC: autoscale the worker pool size up to maxNumWorkers until the job completes.")
  @Default.Enum("NONE")
  @Experimental(Experimental.Kind.AUTOSCALING)
  AutoscalingAlgorithmType getAutoscalingAlgorithm();
  void setAutoscalingAlgorithm(AutoscalingAlgorithmType value);

  /**
   * The maximum number of workers to use when using workerpool autoscaling.
   */
  @Description("[Experimental] The maximum number of workers to use when using workerpool "
      + "autoscaling.")
  @Default.Integer(20)
  @Experimental(Experimental.Kind.AUTOSCALING)
  int getMaxNumWorkers();
  void setMaxNumWorkers(int value);

  /**
   * Remote worker disk size, in gigabytes, or 0 to use the default size.
   */
  @Description("Remote worker disk size, in gigabytes, or 0 to use the default size.")
  int getDiskSizeGb();
  void setDiskSizeGb(int value);

  /**
   * GCE <a href="https://cloud.google.com/compute/docs/networking">network</a> for launching
   * workers.
   *
   * <p> Default is up to the Dataflow service.
   */
  @Description("GCE network for launching workers. Default is up to the Dataflow service.")
  String getNetwork();
  void setNetwork(String value);

  /**
   * GCE <a href="https://developers.google.com/compute/docs/zones"
   * >availability zone</a> for launching workers.
   *
   * <p> Default is up to the Dataflow service.
   */
  @Description("GCE availability zone for launching workers. "
      + "Default is up to the Dataflow service.")
  String getZone();
  void setZone(String value);

  /**
   * Type of API for handling cluster management, i.e. resizing, healthchecking, etc.
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

  /**
   * Type of API for handling cluster management, i.e. resizing, healthchecking, etc.
   */
  @Description("Type of API for handling cluster management, i.e. resizing, healthchecking, etc.")
  @Default.Enum("COMPUTE_ENGINE")
  ClusterManagerApiType getClusterManagerApi();
  void setClusterManagerApi(ClusterManagerApiType value);

  /**
   * Machine type to create Dataflow worker VMs as.
   * <p>
   * See <a href="https://cloud.google.com/compute/docs/machine-types">GCE machine types</a>
   * for a list of valid options.
   * <p>
   * If unset, the Dataflow service will choose a reasonable default.
   */
  @Description("Machine type to create Dataflow worker VMs as. See "
      + "https://cloud.google.com/compute/docs/machine-types for a list of valid options. "
      + "If unset, the Dataflow service will choose a reasonable default.")
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
   * The teardown policy for the VMs.
   *
   * <p> By default this is left unset and the service sets the default policy.
   */
  @Description("The teardown policy for the VMs. By default this is left unset "
      + "and the service sets the default policy.")
  TeardownPolicy getTeardownPolicy();
  void setTeardownPolicy(TeardownPolicy value);

  /**
   * List of local files to make available to workers.
   * <p>
   * Files are placed on the worker's classpath.
   * <p>
   * The default value is the list of jars from the main program's classpath.
   */
  @Description("Files to stage on GCS and make available to workers. "
      + "Files are placed on the worker's classpath. "
      + "The default value is all files from the classpath.")
  List<String> getFilesToStage();
  void setFilesToStage(List<String> value);

  /**
   * Specifies what type of worker pool should be used.
   * <p> This is an internal option, and should not be set when using the Dataflow service.
   */
  @Description("Specifies what type of worker pool should be used. Should never be modified when "
      + "using the Dataflow service")
  @Default.String("harness")
  String getWorkerPoolType();
  void setWorkerPoolType(String value);

  /**
   * Specifies what type of persistent disk should be used.
   */
  @Description("Specifies what type of persistent disk should be used.")
  @Default.String("compute.googleapis.com/projects//zones//diskTypes/pd-standard")
  String getWorkerDiskType();
  void setWorkerDiskType(String value);
}
