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
package org.apache.beam.it.gcp.bigquery.conditions;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.TableId;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.checkerframework.checker.nullness.qual.Nullable;

/** ConditionCheck to validate if BigQuery has received a certain number of rows. */
@AutoValue
public abstract class BigQueryRowsCheck extends ConditionCheck {

  abstract BigQueryResourceManager resourceManager();

  abstract TableId tableId();

  abstract Integer minRows();

  abstract @Nullable Integer maxRows();

  @Override
  public String getDescription() {
    if (maxRows() != null) {
      return String.format(
          "BigQuery check if table %s has between %d and %d rows",
          tableId().getTable(), minRows(), maxRows());
    }
    return String.format("BigQuery check if table %s has %d rows", tableId().getTable(), minRows());
  }

  @Override
  @SuppressWarnings("unboxing.of.nullable")
  public CheckResult check() {
    long totalRows = getRowCount();
    if (totalRows < minRows()) {
      return new CheckResult(
          false, String.format("Expected %d but has only %d", minRows(), totalRows));
    }
    if (maxRows() != null && totalRows > maxRows()) {
      return new CheckResult(
          false, String.format("Expected up to %d but found %d rows", maxRows(), totalRows));
    }

    if (maxRows() != null) {
      return new CheckResult(
          true,
          String.format(
              "Expected between %d and %d rows and found %d", minRows(), maxRows(), totalRows));
    }

    return new CheckResult(
        true, String.format("Expected at least %d rows and found %d", minRows(), totalRows));
  }

  public static Builder builder(BigQueryResourceManager resourceManager, TableId tableId) {
    return new AutoValue_BigQueryRowsCheck.Builder()
        .setResourceManager(resourceManager)
        .setTableId(tableId);
  }

  public Long getRowCount() {
    return resourceManager().getRowCount(tableId().getTable());
  }

  /** Builder for {@link BigQueryRowsCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setResourceManager(BigQueryResourceManager resourceManager);

    public abstract Builder setTableId(TableId tableId);

    public abstract Builder setMinRows(Integer minRows);

    public abstract Builder setMaxRows(Integer maxRows);

    abstract BigQueryRowsCheck autoBuild();

    public BigQueryRowsCheck build() {
      return autoBuild();
    }
  }
}
