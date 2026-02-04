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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

/** Represents a BigQuery source description used for dynamic read. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BigQueryDynamicReadDescriptor implements Serializable {
  @SchemaFieldName("query")
  @SchemaFieldNumber("0")
  @Pure
  abstract @Nullable String getQuery();

  @SchemaFieldName("table")
  @SchemaFieldNumber("1")
  @Pure
  abstract @Nullable String getTable();

  @SchemaFieldName("flattenResults")
  @SchemaFieldNumber("2")
  @Pure
  abstract @Nullable Boolean getFlattenResults();

  @SchemaFieldName("legacySql")
  @SchemaFieldNumber("3")
  @Pure
  abstract @Nullable Boolean getUseLegacySql();

  @SchemaFieldName("selectedFields")
  @SchemaFieldNumber("4")
  @Pure
  abstract @Nullable List<String> getSelectedFields();

  @SchemaFieldName("rowRestriction")
  @SchemaFieldNumber("5")
  @Pure
  abstract @Nullable String getRowRestriction();

  @SchemaCreate
  public static BigQueryDynamicReadDescriptor create(
      @Nullable String query,
      @Nullable String table,
      @Nullable Boolean flattenResults,
      @Nullable Boolean useLegacySql,
      @Nullable List<String> selectedFields,
      @Nullable String rowRestriction) {
    checkArgument((query != null || table != null), "Either query or table has to be specified.");
    checkArgument(
        !(query != null && table != null), "Either query or table has to be specified not both.");
    checkArgument(
        !(table != null && (flattenResults != null || useLegacySql != null)),
        "Specifies a table with a result flattening preference or legacySql, which only applies to queries");
    checkArgument(
        !(query != null && (selectedFields != null || rowRestriction != null)),
        "Selected fields and row restriction are only applicable for table reads");
    checkArgument(
        !(query != null && (flattenResults == null || useLegacySql == null)),
        "If query is used, flattenResults and legacySql have to be set as well.");

    return new AutoValue_BigQueryDynamicReadDescriptor(
        query, table, flattenResults, useLegacySql, selectedFields, rowRestriction);
  }

  public static BigQueryDynamicReadDescriptor query(
      String query, Boolean flattenResults, Boolean useLegacySql) {
    return create(query, null, flattenResults, useLegacySql, null, null);
  }

  public static BigQueryDynamicReadDescriptor table(
      String table, @Nullable List<String> selectedFields, @Nullable String rowRestriction) {
    return create(null, table, null, null, selectedFields, rowRestriction);
  }
}
