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
package org.apache.beam.it.common.logging;

import com.google.api.client.json.GenericJson;
import com.google.api.services.dataflow.model.Job;
import com.google.gson.GsonBuilder;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** Utility for formatting different objects for easier readability in logs. */
public final class LogStrings {
  private LogStrings() {}

  /** Formats a Google API's {@link GenericJson} for pretty logging. */
  public static String formatForLogging(GenericJson genericJson) {
    return formatForLogging(ImmutableMap.copyOf(genericJson));
  }

  /**
   * Formats a Dataflow {@link Job} for pretty logging.
   *
   * <p>Some information will be excluded from the logs in order to improve readability and avoid
   * hitting log limits.
   */
  public static String formatForLogging(Job job) {
    // The environment and steps can really pollute the logging output, making it hard to read
    // and potentially causing problems on systems with limits to how much logging is allowed.
    Job simpleCopy =
        new Job()
            .setId(job.getId())
            .setName(job.getName())
            .setProjectId(job.getProjectId())
            .setLocation(job.getLocation())
            .setCreateTime(job.getCreateTime())
            .setCurrentStateTime(job.getCurrentStateTime())
            .setRequestedState(job.getRequestedState()) // For when we try to cancel it
            .setCurrentState(job.getCurrentState())
            .setLabels(job.getLabels())
            .setJobMetadata(job.getJobMetadata())
            .setType(job.getType());
    return formatForLogging(ImmutableMap.copyOf(simpleCopy));
  }

  /** Formats a map for pretty logging. */
  public static <K, V> String formatForLogging(Map<K, V> map) {
    return new GsonBuilder().setPrettyPrinting().create().toJson(map);
  }
}
