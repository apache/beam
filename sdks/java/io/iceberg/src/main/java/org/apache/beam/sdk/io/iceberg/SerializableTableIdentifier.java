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
package org.apache.beam.sdk.io.iceberg;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
abstract class SerializableTableIdentifier implements Serializable {
  private static final SerializableFunction<SerializableTableIdentifier, Row> TO_ROW_FUNCTION;
  private static final SerializableFunction<Row, SerializableTableIdentifier> FROM_ROW_FUNCTION;
  static final Schema SCHEMA;

  static {
    try {
      SchemaRegistry registry = SchemaRegistry.createDefault();
      SCHEMA = registry.getSchema(SerializableTableIdentifier.class);
      TO_ROW_FUNCTION = registry.getToRowFunction(SerializableTableIdentifier.class);
      FROM_ROW_FUNCTION = registry.getFromRowFunction(SerializableTableIdentifier.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }

  Row toRow() {
    return TO_ROW_FUNCTION.apply(this);
  }

  static SerializableTableIdentifier fromRow(Row row) {
    return FROM_ROW_FUNCTION.apply(row);
  }

  abstract List<String> getNamespace();

  abstract String getTableName();

  static SerializableTableIdentifier of(TableIdentifier tableIdentifier) {
    List<String> namespace = Arrays.asList(tableIdentifier.namespace().levels());
    String tableName = tableIdentifier.name();

    return SerializableTableIdentifier.builder()
        .setNamespace(namespace)
        .setTableName(tableName)
        .build();
  }

  @SchemaIgnore
  TableIdentifier toTableIdentifier() {
    String[] levels = getNamespace().toArray(new String[0]);
    return TableIdentifier.of(Namespace.of(levels), getTableName());
  }

  public static Builder builder() {
    return new AutoValue_SerializableTableIdentifier.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setNamespace(List<String> namespace);

    abstract Builder setTableName(String tableName);

    abstract SerializableTableIdentifier build();
  }
}
