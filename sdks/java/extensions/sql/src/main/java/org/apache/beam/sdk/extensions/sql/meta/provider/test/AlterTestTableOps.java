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
package org.apache.beam.sdk.extensions.sql.meta.provider.test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.beam.sdk.extensions.sql.meta.provider.AlterTableOps;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.TableWithRows;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class AlterTestTableOps implements AlterTableOps {
  private final TableWithRows tableWithRows;

  AlterTestTableOps(TableWithRows tableWithRows) {
    this.tableWithRows = tableWithRows;
  }

  @Override
  public void updateTableProperties(Map<String, String> setProps, List<String> resetProps) {
    ObjectNode props = tableWithRows.getTable().getProperties();
    resetProps.forEach(props::remove);
    setProps.forEach(props::put);
    tableWithRows.setTable(tableWithRows.getTable().toBuilder().properties(props).build());
  }

  @Override
  public void updateSchema(List<Field> columnsToAdd, Collection<String> columnsToDrop) {
    if (!columnsToAdd.isEmpty() && !tableWithRows.getRows().isEmpty()) {
      ImmutableList.Builder<String> requiredFields = ImmutableList.builder();
      for (Field f : columnsToAdd) {
        if (!f.getType().getNullable()) {
          requiredFields.add(f.getName());
        }
      }
      Preconditions.checkArgument(
          requiredFields.build().isEmpty(),
          "Cannot add required fields %s because table '%s' already contains rows.",
          requiredFields.build(),
          tableWithRows.getTable().getName());
    }

    // update the schema
    List<Field> schemaFields = tableWithRows.getTable().getSchema().getFields();
    ImmutableList.Builder<Field> newSchemaFields = ImmutableList.builder();
    // remove dropped fields
    schemaFields.stream()
        .filter(f -> !columnsToDrop.contains(f.getName()))
        .forEach(newSchemaFields::add);
    // add new fields
    newSchemaFields.addAll(columnsToAdd);
    Schema newSchema = Schema.of(newSchemaFields.build().toArray(new Field[0]));
    tableWithRows.setTable(tableWithRows.getTable().toBuilder().schema(newSchema).build());

    // update existing rows
    List<Row> rows = tableWithRows.getRows();
    List<Row> newRows = new CopyOnWriteArrayList<>();
    for (Row row : rows) {
      Map<String, Object> values = new HashMap<>();
      // add existing values, minus dropped columns
      for (Field field : schemaFields) {
        String name = field.getName();
        if (!columnsToDrop.contains(name)) {
          values.put(name, row.getValue(name));
        }
      }
      Row newRow = Row.withSchema(newSchema).withFieldValues(values).build();
      newRows.add(newRow);
    }
    tableWithRows.setRows(newRows);
  }

  @Override
  public void updatePartitionSpec(
      List<String> partitionsToAdd, Collection<String> partitionsToDrop) {
    throw new UnsupportedOperationException(
        TestTableProvider.class.getSimpleName() + " does not support partitions.");
  }
}
