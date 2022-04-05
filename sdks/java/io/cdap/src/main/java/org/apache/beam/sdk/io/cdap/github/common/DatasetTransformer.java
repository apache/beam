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
package org.apache.beam.sdk.io.cdap.github.common;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.cdap.github.common.model.GitHubModel;

/**
 * This is helper class for transforming {@link GitHubModel} instance to {@link StructuredRecord}.
 */
public class DatasetTransformer {

  /**
   * Transforms {@link GitHubModel} instance to {@link StructuredRecord} instance accordingly to
   * given schema.
   */
  public static StructuredRecord transform(Object model, Schema schema) {
    try {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      Class<?> clazz = model.getClass();
      List<Schema.Field> schemaFields = schema.getFields();
      for (Schema.Field schemaField : schemaFields) {
        Schema.Type schemaType = schemaField.getSchema().getType();
        /**
         * TODO - Check for Field NULL and implement validation
         * https://issues.cask.co/browse/PLUGIN-383
         */
        Field field = getFieldByName(schemaField.getName(), clazz);
        if (schemaType.isSimpleType() || Schema.Type.UNION.equals(schemaType)) {
          builder.set(schemaField.getName(), field.get(model));
        } else if (Schema.Type.ARRAY.equals(schemaType)) {
          Schema componentSchema = schemaField.getSchema().getComponentSchema();
          Object result = field.get(model);
          if (Objects.nonNull(componentSchema) && !componentSchema.isSimpleOrNullableSimple()) {
            result =
                ((List<?>) result)
                    .stream()
                        .map(arrItem -> transform(arrItem, componentSchema))
                        .collect(Collectors.toList());
          }
          builder.set(schemaField.getName(), result);
        } else {
          StructuredRecord structuredRecord = transform(field.get(model), schemaField.getSchema());
          builder.set(schemaField.getName(), structuredRecord);
        }
      }
      return builder.build();
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(
          String.format(
              "Exception when transforming %s to StructuredRecord",
              model.getClass().getSimpleName()),
          e);
    }
  }

  private static Field getFieldByName(String fieldName, Class<?> clazz) {
    Field declaredField = null;
    Class<?> currentClass = clazz;
    while (currentClass != Object.class) {
      try {
        declaredField = currentClass.getDeclaredField(fieldName);
        declaredField.setAccessible(true);
        return declaredField;
      } catch (NoSuchFieldException e) {
        currentClass = currentClass.getSuperclass();
      }
    }
    return declaredField;
  }
}
