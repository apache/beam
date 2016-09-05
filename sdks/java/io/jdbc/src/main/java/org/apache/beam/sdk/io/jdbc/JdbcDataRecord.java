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
package org.apache.beam.sdk.io.jdbc;

import java.io.Serializable;

/**
 * Container for JDBC data, providing the data details (column name, table name, payload, data
 * type).
 */
public class JdbcDataRecord implements Serializable {

  private String[] tableNames;
  private String[] columnNames;
  private Object[] columnValues;
  private int[] columnTypes;

  public JdbcDataRecord() {
  }

  public JdbcDataRecord(int size) {
    this.tableNames = new String[size];
    this.columnNames = new String[size];
    this.columnValues = new Object[size];
    this.columnTypes = new int[size];
  }

  public String[] getTableNames() {
    return tableNames;
  }

  public void setTableNames(String[] tableName) {
    this.tableNames = tableName;
  }

  public String[] getColumnNames() {
    return columnNames;
  }

  public void setColumnNames(String[] columnNames) {
    this.columnNames = columnNames;
  }

  public Object[] getColumnValues() {
    return columnValues;
  }

  public void setColumnValues(Object[] columnValues) {
    this.columnValues = columnValues;
  }

  public int[] getColumnTypes() {
    return columnTypes;
  }

  public void setColumnTypes(int[] columnTypes) {
    this.columnTypes = columnTypes;
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < tableNames.length; i++) {
      builder.append("Table: ").append(tableNames[i]).append(" | Column: ")
          .append(columnNames[i]).append(" | Type: ").append(columnTypes[i])
          .append(" | Value: ").append(columnValues[i]).append("\n");
    }
    return builder.toString();
  }

}
