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
package org.apache.beam.runners.samza;

import java.time.Duration;

// TODO: can we get rid of this class? Right now the SamzaPipelineOptionsValidator would force
// the pipeline option to be the type SamzaPipelineOption. Ideally, we should be able to keep
// passing SamzaPortablePipelineOption. Alternative, we could merge portable and non-portable
// pipeline option.
/** A helper class for holding all the beam runner specific samza configs. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class SamzaRunnerOverrideConfigs {
  public static final String BEAM_RUNNER_CONFIG_PREFIX = "beam.override.";
  // whether the job is in portable mode
  public static final String IS_PORTABLE_MODE = BEAM_RUNNER_CONFIG_PREFIX + "portable";
  // for portable mode only: port number for fn control api
  public static final String FN_CONTROL_PORT = BEAM_RUNNER_CONFIG_PREFIX + "control.port";
  // timeout for waiting for control client to connect
  public static final String CONTROL_CLIENT_MAX_WAIT_TIME_MS = "controL.wait.time.ms";
  public static final long DEFAULT_CONTROL_CLIENT_MAX_WAIT_TIME_MS =
      Duration.ofMinutes(2).toMillis();
  public static final String FS_TOKEN_PATH = BEAM_RUNNER_CONFIG_PREFIX + "fs.token.path";
  public static final String DEFAULT_FS_TOKEN_PATH = null;

  private static boolean containsKey(SamzaPipelineOptions options, String configKey) {
    if (options == null || options.getConfigOverride() == null) {
      return false;
    }
    return options.getConfigOverride().containsKey(configKey);
  }

  /** Whether the job is in portable mode based on the config override in the pipeline options. */
  public static boolean isPortableMode(SamzaPipelineOptions options) {
    if (containsKey(options, IS_PORTABLE_MODE)) {
      return options.getConfigOverride().get(IS_PORTABLE_MODE).equals(String.valueOf(true));
    } else {
      return false;
    }
  }

  /** Get fn control port number based on the config override in the pipeline options. */
  public static int getFnControlPort(SamzaPipelineOptions options) {
    if (containsKey(options, FN_CONTROL_PORT)) {
      return Integer.parseInt(options.getConfigOverride().get(FN_CONTROL_PORT));
    } else {
      return -1;
    }
  }

  /** Get max wait time for control client connection. */
  public static long getControlClientWaitTimeoutMs(SamzaPipelineOptions options) {
    if (containsKey(options, CONTROL_CLIENT_MAX_WAIT_TIME_MS)) {
      return Long.parseLong(options.getConfigOverride().get(CONTROL_CLIENT_MAX_WAIT_TIME_MS));
    } else {
      return DEFAULT_CONTROL_CLIENT_MAX_WAIT_TIME_MS;
    }
  }

  /** Get fs token path for portable mode. */
  public static String getFsTokenPath(SamzaPipelineOptions options) {
    if (containsKey(options, FS_TOKEN_PATH)) {
      return options.getConfigOverride().get(FS_TOKEN_PATH);
    } else {
      return DEFAULT_FS_TOKEN_PATH;
    }
  }
}
