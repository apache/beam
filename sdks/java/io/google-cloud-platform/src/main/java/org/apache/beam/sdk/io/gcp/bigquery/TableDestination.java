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

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.io.Serializable;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Encapsulates a BigQuery table destination. */
public class TableDestination implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String tableSpec;
  private final @Nullable String tableDescription;
  private final @Nullable String jsonTimePartitioning;
  private final @Nullable String jsonClustering;

  public TableDestination(String tableSpec, @Nullable String tableDescription) {
    this(tableSpec, tableDescription, (String) null, (String) null);
  }

  public TableDestination(TableReference tableReference, @Nullable String tableDescription) {
    this(tableReference, tableDescription, (String) null, (String) null);
  }

  public TableDestination(
      TableReference tableReference,
      @Nullable String tableDescription,
      TimePartitioning timePartitioning) {
    this(
        BigQueryHelpers.toTableSpec(tableReference),
        tableDescription,
        timePartitioning != null ? BigQueryHelpers.toJsonString(timePartitioning) : null,
        (String) null);
  }

  public TableDestination(
      String tableSpec, @Nullable String tableDescription, TimePartitioning timePartitioning) {
    this(
        tableSpec,
        tableDescription,
        timePartitioning != null ? BigQueryHelpers.toJsonString(timePartitioning) : null,
        (String) null);
  }

  public TableDestination(
      String tableSpec,
      @Nullable String tableDescription,
      TimePartitioning timePartitioning,
      Clustering clustering) {
    this(
        tableSpec,
        tableDescription,
        timePartitioning != null ? BigQueryHelpers.toJsonString(timePartitioning) : null,
        clustering != null ? BigQueryHelpers.toJsonString(clustering) : null);
  }

  public TableDestination(
      String tableSpec, @Nullable String tableDescription, @Nullable String jsonTimePartitioning) {
    this(tableSpec, tableDescription, jsonTimePartitioning, (String) null);
  }

  public TableDestination(
      TableReference tableReference,
      @Nullable String tableDescription,
      @Nullable String jsonTimePartitioning) {
    this(
        BigQueryHelpers.toTableSpec(tableReference),
        tableDescription,
        jsonTimePartitioning,
        (String) null);
  }

  public TableDestination(
      TableReference tableReference,
      @Nullable String tableDescription,
      @Nullable String jsonTimePartitioning,
      @Nullable String jsonClustering) {
    this(
        BigQueryHelpers.toTableSpec(tableReference),
        tableDescription,
        jsonTimePartitioning,
        jsonClustering);
  }

  public TableDestination(
      String tableSpec,
      @Nullable String tableDescription,
      @Nullable String jsonTimePartitioning,
      @Nullable String jsonClustering) {
    this.tableSpec = tableSpec;
    this.tableDescription = tableDescription;
    this.jsonTimePartitioning = jsonTimePartitioning;
    this.jsonClustering = jsonClustering;
  }

  public TableDestination withTableReference(TableReference tableReference) {
    return new TableDestination(
        tableReference, tableDescription, jsonTimePartitioning, jsonClustering);
  }

  public String getTableSpec() {
    return tableSpec;
  }

  public TableReference getTableReference() {
    return BigQueryHelpers.parseTableSpec(tableSpec);
  }

  public String getJsonTimePartitioning() {
    return jsonTimePartitioning;
  }

  public TimePartitioning getTimePartitioning() {
    if (jsonTimePartitioning == null) {
      return null;
    } else {
      return BigQueryHelpers.fromJsonString(jsonTimePartitioning, TimePartitioning.class);
    }
  }

  public String getJsonClustering() {
    return jsonClustering;
  }

  public Clustering getClustering() {
    if (jsonClustering == null) {
      return null;
    } else {
      return BigQueryHelpers.fromJsonString(jsonClustering, Clustering.class);
    }
  }

  public @Nullable String getTableDescription() {
    return tableDescription;
  }

  @Override
  public String toString() {
    String toString = "tableSpec: " + tableSpec;
    if (tableDescription != null) {
      toString += " tableDescription: " + tableDescription;
    }
    return toString;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TableDestination)) {
      return false;
    }
    TableDestination other = (TableDestination) o;
    return Objects.equals(this.tableSpec, other.tableSpec)
        && Objects.equals(this.tableDescription, other.tableDescription)
        && Objects.equals(this.jsonTimePartitioning, other.jsonTimePartitioning);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableSpec, tableDescription, jsonTimePartitioning);
  }
}
