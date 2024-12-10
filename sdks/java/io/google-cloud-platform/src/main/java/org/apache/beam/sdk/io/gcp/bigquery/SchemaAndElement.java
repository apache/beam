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

import com.google.api.services.bigquery.model.TableSchema;

/**
 * A wrapper for a record and the {@link TableSchema} representing the schema of the table (or
 * query) it was generated from.
 */
public class SchemaAndElement<T> {
  private final T element;
  private final TableSchema tableSchema;

  public SchemaAndElement(T record, TableSchema tableSchema) {
    this.element = record;
    this.tableSchema = tableSchema;
  }

  public T getElement() {
    return element;
  }

  // getRecord is defined here so method is present when cast to SchemaAndRecord
  public T getRecord() {
    return element;
  }

  // getRow is defined here so method is present when cast to SchemaAndRow
  protected T getRow() {
    return element;
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }
}
