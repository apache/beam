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
package org.apache.beam.it.truthmatchers;

import static com.google.common.truth.Truth.assertAbout;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Assert utilities for Template DSL-like tests. */
public class PipelineAsserts {

  /**
   * Creates a {@link LaunchInfoSubject} to assert information returned from pipeline launches.
   *
   * @param launchInfo Launch information returned from the launcher.
   * @return Truth Subject to chain assertions.
   */
  public static LaunchInfoSubject assertThatPipeline(LaunchInfo launchInfo) {
    return assertAbout(LaunchInfoSubject.launchInfo()).that(launchInfo);
  }

  /**
   * Creates a {@link ResultSubject} to add assertions based on a pipeline result.
   *
   * @param result Pipeline result returned from the launcher.
   * @return Truth Subject to chain assertions.
   */
  public static ResultSubject assertThatResult(Result result) {
    return assertAbout(ResultSubject.result()).that(result);
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param records Records in a map list format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatRecords(@Nullable List<Map<String, Object>> records) {
    return assertAbout(RecordsSubject.records()).that(records);
  }

  /**
   * Convert JSON Strings to a list of maps.
   *
   * @param jsonRecords List of JSON Strings to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> jsonRecordsToRecords(
      @Nullable Iterable<String> jsonRecords) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();
      if (jsonRecords != null) {
        jsonRecords.forEach(
            json -> records.add(new Gson().<Map<String, Object>>fromJson(json, Map.class)));
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting JSON Records to Records", e);
    }
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of JSON Strings.
   *
   * @param jsonRecords Strings in JSON format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatJsonRecords(@Nullable List<String> jsonRecords) {
    return assertThatRecords(jsonRecordsToRecords(jsonRecords));
  }
}
