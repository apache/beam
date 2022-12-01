/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.matchers;

import static com.google.common.truth.Truth.assertAbout;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.it.artifacts.Artifact;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/** Assert utilities for Template DSL-like tests. */
public final class TemplateAsserts {

  private static TypeReference<Map<String, Object>> recordTypeReference = new TypeReference<>() {};

  private static ObjectMapper objectMapper = new ObjectMapper();

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
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param tableResult Records in BigQuery TableResult format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatRecords(@Nullable TableResult tableResult) {
    return assertThatRecords(tableResultToRecords(tableResult));
  }

  /**
   * Creates a {@link ArtifactsSubject} to assert information within a list of artifacts obtained
   * from Cloud Storage.
   *
   * @param artifacts Artifacts in list format to use in the comparisons.
   * @return Truth Subject to chain assertions.
   */
  public static ArtifactsSubject assertThatArtifacts(@Nullable List<Artifact> artifacts) {
    return assertAbout(ArtifactsSubject.records()).that(artifacts);
  }

  /**
   * Convert BigQuery {@link TableResult} to a list of maps.
   *
   * @param tableResult Table Result to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  private static List<Map<String, Object>> tableResultToRecords(TableResult tableResult) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (FieldValueList row : tableResult.iterateAll()) {
        String jsonRow = row.get(0).getStringValue();
        Map<String, Object> converted = objectMapper.readValue(jsonRow, recordTypeReference);
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting TableResult to Records", e);
    }
  }
}
