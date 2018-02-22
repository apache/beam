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

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Encapsulates a BigQuery table destination.
 */
public class TableDestination implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String tableSpec;
  @Nullable
  private final String tableDescription;
  @Nullable
  private final String jsonTimePartitioning;


  public TableDestination(String tableSpec, @Nullable String tableDescription) {
    this(tableSpec, tableDescription, (String) null);
  }

  public TableDestination(TableReference tableReference, @Nullable String tableDescription) {
    this(tableReference, tableDescription, (String) null);
  }

  public TableDestination(TableReference tableReference, @Nullable String tableDescription,
      TimePartitioning timePartitioning) {
    this(BigQueryHelpers.toTableSpec(tableReference), tableDescription,
        timePartitioning != null ? BigQueryHelpers.toJsonString(timePartitioning) : null);
  }

  public TableDestination(String tableSpec, @Nullable String tableDescription,
      TimePartitioning timePartitioning) {
    this(tableSpec, tableDescription,
        timePartitioning != null ? BigQueryHelpers.toJsonString(timePartitioning) : null);
  }

  public TableDestination(TableReference tableReference, @Nullable String tableDescription,
      @Nullable String jsonTimePartitioning) {
    this(BigQueryHelpers.toTableSpec(tableReference), tableDescription, jsonTimePartitioning);
  }

  public TableDestination(String tableSpec, @Nullable String tableDescription,
      @Nullable String jsonTimePartitioning) {
    this.tableSpec = tableSpec;
    this.tableDescription = tableDescription;
    this.jsonTimePartitioning = jsonTimePartitioning;
  }

  public TableDestination withTableReference(TableReference tableReference) {
    return new TableDestination(tableReference, tableDescription, jsonTimePartitioning);
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

  @Nullable
  public String getTableDescription() {
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
