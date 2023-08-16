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
package org.apache.beam.it.gcp.bigquery;

import static org.apache.beam.it.common.utils.ResourceManagerUtils.generateResourceId;

import com.google.cloud.bigquery.TableId;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

/** Utilities for {@link BigQueryResourceManager} implementations. */
public final class BigQueryResourceManagerUtils {

  private static final int MAX_DATASET_ID_LENGTH = 1024;
  private static final Pattern ILLEGAL_DATASET_ID_CHARS = Pattern.compile("[^a-zA-Z0-9_]");
  private static final int MIN_TABLE_ID_LENGTH = 1;
  private static final int MAX_TABLE_ID_LENGTH = 1024;
  private static final Pattern ILLEGAL_TABLE_CHARS = Pattern.compile("[^a-zA-Z0-9-_]");
  private static final DateTimeFormatter TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSSSSS");

  private BigQueryResourceManagerUtils() {}

  /**
   * Utility function to generate a formatted dataset name.
   *
   * <p>A dataset name must contain only alphanumeric characters and underscores with a max length
   * of 1024.
   *
   * @param datasetName the dataset name to be formatted into a valid ID.
   * @return a BigQuery compatible dataset name.
   */
  static String generateDatasetId(String datasetName) {
    return generateResourceId(
        datasetName, ILLEGAL_DATASET_ID_CHARS, "_", MAX_DATASET_ID_LENGTH, TIME_FORMAT);
  }

  /**
   * Checks whether the given table ID is valid according to GCP constraints.
   *
   * @param idToCheck the table ID to check.
   * @throws IllegalArgumentException if the table ID is invalid.
   */
  static void checkValidTableId(String idToCheck) {
    if (idToCheck.length() < MIN_TABLE_ID_LENGTH) {
      throw new IllegalArgumentException("Table ID cannot be empty. ");
    }
    if (idToCheck.length() > MAX_TABLE_ID_LENGTH) {
      throw new IllegalArgumentException(
          "Table ID "
              + idToCheck
              + " cannot be longer than "
              + MAX_TABLE_ID_LENGTH
              + " characters.");
    }
    if (ILLEGAL_TABLE_CHARS.matcher(idToCheck).find()) {
      throw new IllegalArgumentException(
          "Table ID "
              + idToCheck
              + " is not a valid ID. Only letters, numbers, hyphens and underscores are allowed.");
    }
  }

  /**
   * Convert a BigQuery TableId to a table spec string.
   *
   * @param project project in which the table exists.
   * @param table TableId to format.
   * @return String in the format {project}:{dataset}.{table}.
   */
  public static String toTableSpec(String project, TableId table) {
    return String.format(
        "%s:%s.%s",
        table.getProject() != null ? table.getProject() : project,
        table.getDataset(),
        table.getTable());
  }
}
