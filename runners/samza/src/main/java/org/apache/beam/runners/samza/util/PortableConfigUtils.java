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

import java.util.Map;
import org.apache.beam.runners.samza.SamzaPipelineOptions;

/** A utility class to encapsulate. */
public class PortableConfigUtils {
  public static final String BEAM_PORTABLE_MODE = "beam.portable.mode";

  private PortableConfigUtils() {}

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

    return Boolean.parseBoolean(override.getOrDefault(BEAM_PORTABLE_MODE, "false"));
  }
}
