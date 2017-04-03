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

import java.io.Serializable;
import java.util.Objects;

/**
 * Encapsulates a BigQuery table destination.
 */
public class TableDestination implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String tableSpec;
  private final String tableDescription;


  public TableDestination(String tableSpec, String tableDescription) {
    this.tableSpec = tableSpec;
    this.tableDescription = tableDescription;
  }

  public TableDestination(TableReference tableReference, String tableDescription) {
    this.tableSpec = BigQueryHelpers.toTableSpec(tableReference);
    this.tableDescription = tableDescription;
  }

  public String getTableSpec() {
    return tableSpec;
  }

  public TableReference getTableReference() {
    return BigQueryHelpers.parseTableSpec(tableSpec);
  }

  public String getTableDescription() {
    return tableDescription;
  }

  @Override
  public String toString() {
    return "tableSpec: " + tableSpec + " tableDescription: " + tableDescription;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TableDestination)) {
      return false;
    }
    TableDestination other = (TableDestination) o;
    return Objects.equals(this.tableSpec, other.tableSpec)
        && Objects.equals(this.tableDescription, other.tableDescription);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableSpec, tableDescription);
  }
}
