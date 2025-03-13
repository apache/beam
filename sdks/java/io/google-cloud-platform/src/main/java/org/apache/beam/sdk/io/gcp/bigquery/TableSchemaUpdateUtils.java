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

import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;

/** Helper utilities for handling schema-update responses. */
public class TableSchemaUpdateUtils {
  /*
  Given an original schema and an updated schema, return a schema that should be used to process future records.
  This function returns:
      - If the new schema is not compatible (e.g. missing fields), then it will return Optional.empty().
      - If the new schema is equivalent (i.e. equal modulo field ordering) to the old schema, then it will return
        Optional.empty().
      - The returned schema will always contain the old schema as a prefix. This ensures that if any of the old
       fields are reordered in the new schema, we maintain the old order.
   */
  public static Optional<TableSchema> getUpdatedSchema(
      TableSchema oldSchema, TableSchema newSchema) {
    Result updatedFields = getUpdatedSchema(oldSchema.getFieldsList(), newSchema.getFieldsList());
    if (updatedFields.isEquivalent()) {
      return Optional.empty();
    } else {
      return updatedFields
          .getFields()
          .map(
              tableFieldSchemas ->
                  TableSchema.newBuilder().addAllFields(tableFieldSchemas).build());
    }
  }

  @AutoValue
  abstract static class Result {
    abstract Optional<List<TableFieldSchema>> getFields();

    abstract boolean isEquivalent();

    static Result of(List<TableFieldSchema> fields, boolean isEquivalent) {
      return new AutoValue_TableSchemaUpdateUtils_Result(Optional.of(fields), isEquivalent);
    }

    static Result empty() {
      return new AutoValue_TableSchemaUpdateUtils_Result(Optional.empty(), false);
    }
  }

  private static Result getUpdatedSchema(
      @Nullable List<TableFieldSchema> oldSchema, @Nullable List<TableFieldSchema> newSchema) {
    if (newSchema == null) {
      return Result.empty();
    }
    if (oldSchema == null) {
      return Result.of(newSchema, false);
    }

    // BigQuery column names are not case-sensitive, but Map keys are.
    Map<String, TableFieldSchema> newSchemaMap =
        newSchema.stream().collect(Collectors.toMap(tr -> tr.getName().toLowerCase(), x -> x));
    Set<String> fieldNamesPopulated = Sets.newHashSet();
    List<TableFieldSchema> updatedSchema = Lists.newArrayList();
    boolean isEquivalent = oldSchema.size() == newSchema.size();
    for (TableFieldSchema tableFieldSchema : oldSchema) {
      @Nullable
      TableFieldSchema newTableFieldSchema =
          newSchemaMap.get(tableFieldSchema.getName().toLowerCase());
      if (newTableFieldSchema == null) {
        // We don't support deleting fields!
        return Result.empty();
      }
      TableFieldSchema.Builder updatedTableFieldSchema = newTableFieldSchema.toBuilder();
      updatedTableFieldSchema.clearFields();
      if (tableFieldSchema.getType().equals(TableFieldSchema.Type.STRUCT)) {
        Result updatedTableFields =
            getUpdatedSchema(tableFieldSchema.getFieldsList(), newTableFieldSchema.getFieldsList());
        if (!updatedTableFields.getFields().isPresent()) {
          return updatedTableFields;
        }
        updatedTableFieldSchema.addAllFields(updatedTableFields.getFields().get());
        isEquivalent = isEquivalent && updatedTableFields.isEquivalent();
        isEquivalent =
            isEquivalent
                && tableFieldSchema
                    .toBuilder()
                    .clearFields()
                    .build()
                    .equals(newTableFieldSchema.toBuilder().clearFields().build());
      } else {
        isEquivalent = isEquivalent && tableFieldSchema.equals(newTableFieldSchema);
      }
      updatedSchema.add(updatedTableFieldSchema.build());
      fieldNamesPopulated.add(updatedTableFieldSchema.getName());
    }

    // Add in new fields at the end of the schema.
    newSchema.stream()
        .filter(f -> !fieldNamesPopulated.contains(f.getName()))
        .forEach(updatedSchema::add);
    return Result.of(updatedSchema, isEquivalent);
  }
}
