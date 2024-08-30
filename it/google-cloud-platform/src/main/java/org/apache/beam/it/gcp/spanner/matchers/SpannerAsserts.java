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
package org.apache.beam.it.gcp.spanner.matchers;

import static org.apache.beam.it.gcp.artifacts.utils.JsonTestUtil.parseJsonString;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Value;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.truthmatchers.RecordsSubject;

public class SpannerAsserts {

  /**
   * Convert Spanner {@link Struct} list to a list of maps.
   *
   * @param structs Structs to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> structsToRecords(List<Struct> structs) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (Struct struct : structs) {
        Map<String, Object> record = new HashMap<>();

        for (Type.StructField field : struct.getType().getStructFields()) {
          Value fieldValue = struct.getValue(field.getName());
          // May need to explore using typed methods instead of .toString()
          record.put(field.getName(), fieldValue.toString());
        }

        records.add(record);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting TableResult to Records", e);
    }
  }

  /**
   * Convert Spanner {@link Mutation} list to a list of maps.
   *
   * @param mutations Mutations to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> mutationsToRecords(List<Mutation> mutations) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();
      mutations.forEach(
          entry ->
              records.add(
                  entry.asMap().entrySet().stream()
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting TableResult to Records", e);
    }
  }

  /**
   * Convert a list of Spanner {@link Mutation} objects into a list of maps, extracting specified
   * columns.
   *
   * @param mutations The list of mutations to process.
   * @param columns The columns to extract.
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> mutationsToRecords(
      List<Mutation> mutations, List<String> columns) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();
      mutations.forEach(
          entry -> {
            records.add(
                entry.asMap().entrySet().stream()
                    .filter((e) -> columns.contains(e.getKey()))
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey,
                            (e) -> {
                              if (e.getValue().getType().getCode() == Code.ARRAY) {
                                return e.getValue().getAsStringList();
                              }
                              if (Arrays.asList(Code.JSON, Code.PG_JSONB)
                                  .contains(e.getValue().getType().getCode())) {
                                try {
                                  return parseJsonString(e.getValue().getJson());
                                } catch (IOException ex) {
                                  throw new RuntimeException(ex);
                                }
                              }
                              return e.getValue().getAsString();
                            })));
          });
      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting TableResult to Records", e);
    }
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param structs Records in Spanner {@link Struct} format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatStructs(List<Struct> structs) {
    return assertThatRecords(structsToRecords(structs));
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param mutations Mutations in Spanner {@link Mutation} format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatMutations(List<Mutation> mutations) {
    return assertThatRecords(mutationsToRecords(mutations));
  }
}
