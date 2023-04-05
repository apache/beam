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

import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Model definition for BigQueryInsertError.
 *
 * <p>This class represents an error inserting a {@link TableRow} into BigQuery.
 */
public class BigQueryInsertError {

  /** The {@link TableRow} that could not be inserted. */
  private TableRow row;

  /**
   * The {@link com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors}
   * caused.
   */
  private TableDataInsertAllResponse.InsertErrors error;

  /**
   * The {@link TableReference} where the {@link BigQueryInsertError#row} was tried to be inserted.
   */
  private TableReference table;

  public BigQueryInsertError(
      TableRow row, TableDataInsertAllResponse.InsertErrors error, TableReference table) {
    this.row = row;
    this.error = error;
    this.table = table;
  }

  public TableRow getRow() {
    return row;
  }

  public TableDataInsertAllResponse.InsertErrors getError() {
    return error;
  }

  public TableReference getTable() {
    return table;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BigQueryInsertError that = (BigQueryInsertError) o;
    return Objects.equals(row, that.getRow())
        && Objects.equals(error, that.getError())
        && Objects.equals(table, that.getTable());
  }

  @Override
  public int hashCode() {
    return Objects.hash(row, error, table);
  }
}
