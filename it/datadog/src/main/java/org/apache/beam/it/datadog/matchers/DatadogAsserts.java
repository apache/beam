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
package org.apache.beam.it.datadog.matchers;

import static org.apache.beam.it.datadog.DatadogResourceManagerUtils.datadogEntryToMap;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.it.datadog.DatadogLogEntry;
import org.apache.beam.it.truthmatchers.RecordsSubject;

/** Assert utilities for Datadog tests. */
public class DatadogAsserts {

  /**
   * Convert Datadog {@link DatadogLogEntry} to a list of maps.
   *
   * @param entries List of com.google.cloud.teleport.it.datadog.DatadogLogEntrys to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> datadogEntriesToRecords(
      Collection<DatadogLogEntry> entries) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (DatadogLogEntry entry : entries) {
        Map<String, Object> converted = datadogEntryToMap(entry);
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting DatadogLogEntries to Records", e);
    }
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param entries List of DatadogLogEntrys in Datadog {@link DatadogLogEntry} format to use in the
   *     comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatDatadogLogEntries(
      @Nullable Collection<DatadogLogEntry> entries) {
    if (entries == null) {
      return assertThatRecords(new ArrayList<>());
    }
    return assertThatRecords(datadogEntriesToRecords(entries));
  }
}
