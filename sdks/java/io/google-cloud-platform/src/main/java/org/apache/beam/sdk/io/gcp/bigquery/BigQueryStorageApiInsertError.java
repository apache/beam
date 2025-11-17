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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import javax.annotation.Nullable;

public class BigQueryStorageApiInsertError {
  private TableRow row;

  private @Nullable String errorMessage;

  private @Nullable String tableUrn;

  private @Nullable String[] parsedParts;

  public BigQueryStorageApiInsertError(TableRow row) {
    this(row, null, null);
  }

  public BigQueryStorageApiInsertError(TableRow row, @Nullable String errorMessage) {
    this(row, errorMessage, null);
  }

  public BigQueryStorageApiInsertError(
      TableRow row, @Nullable String errorMessage, @Nullable String tableUrn) {
    this.row = row;
    this.errorMessage = errorMessage;
    this.tableUrn = tableUrn;
  }

  public TableRow getRow() {
    return row;
  }

  @Nullable
  public String getErrorMessage() {
    return errorMessage;
  }

  @Nullable
  public String getTableUrn() {
    return tableUrn;
  }

  @Nullable
  public String getProjectId() {
    return getParsedPart(1);
  }

  @Nullable
  public String getDatasetId() {
    return getParsedPart(3);
  }

  @Nullable
  public String getTableId() {
    return getParsedPart(5);
  }

  @Nullable
  private String getParsedPart(int index) {
    if (parsedParts == null && tableUrn != null && !tableUrn.isEmpty()) {
      String[] parts = tableUrn.split("/");
      if (parts.length == 6
          && "projects".equals(parts[0])
          && "datasets".equals(parts[2])
          && "tables".equals(parts[4])) {
        parsedParts = parts;
      }
    }
    return parsedParts != null ? parsedParts[index] : null;
  }

  @Override
  public String toString() {
    return "BigQueryStorageApiInsertError{"
        + "row="
        + row
        + ", errorMessage='"
        + errorMessage
        + '\''
        + '}';
  }
}
