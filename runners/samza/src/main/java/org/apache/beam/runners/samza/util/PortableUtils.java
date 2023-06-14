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
package org.apache.beam.runners.samza.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;

/** A utility class to encapsulate. */
public class PortableUtils {
  public static final String BEAM_PORTABLE_MODE = "beam.portable.mode";
  public static final String PORTABLE_JOB_SERVER_ENDPOINT = "job.server.endpoint";

  private PortableUtils() {}

  /**
   * A helper method to distinguish if a pipeline is run using portable mode or classic mode.
   *
   * @param options pipeline options
   * @return true if the pipeline is run in portable mode
   */
  public static boolean isPortable(SamzaPipelineOptions options) {
    Map<String, String> override = options.getConfigOverride();
    if (override == null) {
      return false;
    }

    return Boolean.parseBoolean(override.getOrDefault("beam.portable.mode", "false"));
  }

  public static List<String> getServerDriverArgs() {
    // assigning ports to 0 so that JobServerDriver can dynamically assign ports
    return Arrays.asList("--job-port=0", "--artifact-port=0", "--expansion-port=0");
  }

  public static PipelineOptions getPipelineOptions(JobInfo jobInfo) {
    return PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions());
  }

  /** Updates the Samza portable framework main class runner arguments with jobServerUrl. */
  public static List<String> updateSamzaPortableMainArgs(List<String> args, String jobServerUrl) {

    // set the job server endpoint
    args.add("--config");
    args.add(String.format("%s=%s", PortableUtils.PORTABLE_JOB_SERVER_ENDPOINT, jobServerUrl));

    return args;
  }

  /** Updates the Spark portable framework main class runner arguments with jobServerUrl. */
  public static List<String> updateSparkPortableMainArgs(List<String> args, String jobServerUrl) {

    // set the job server endpoint
    args.add(jobServerUrl);

    return args;
  }

  public static List<String> updateSamzaPortableArgs(String mainClassName, String[] args) {
    List<String> updatedArgs = new ArrayList<>(Arrays.asList(args));
    // Set main class to run
    updatedArgs.add(0, mainClassName);

    // set the flag for portable
    updatedArgs.add("--config");
    updatedArgs.add(String.format("%s=%s", PortableUtils.BEAM_PORTABLE_MODE, "true"));

    return updatedArgs;
  }
}
