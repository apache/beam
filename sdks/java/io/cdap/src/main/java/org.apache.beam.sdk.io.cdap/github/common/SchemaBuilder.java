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

import io.cdap.cdap.api.data.schema.Schema;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Helper class to map GitHub repository fields sets to final {@link Schema}. */
@SuppressWarnings("rawtypes")
public class SchemaBuilder {
  /**
   * Returns selected Schema.
   *
   * @param schemaName the given schema name with
   * @param model the model
   * @return the instance of Schema
   */
  public static Schema buildSchema(String schemaName, Class<?> model) {
    List<Field> fields = getModelFields(model);
    List<Schema.Field> schemaFields = new ArrayList<>();
    for (Field field : fields) {
      Schema.Type schemaType = getSchemaType(field.getType());
      if (schemaType.isSimpleType()) {
        schemaFields.add(getSimpleSchemaField(field));
      } else if (Schema.Type.ARRAY.equals(schemaType)) {
        ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
        Class<?> clazz = (Class<?>) parameterizedType.getActualTypeArguments()[0];
        Schema.Type arraySchemaType = getSchemaType(clazz);
        if (!arraySchemaType.isSimpleType()) {
          Schema schema = buildSchema(field.getName(), clazz);
          schemaFields.add(Schema.Field.of(field.getName(), Schema.arrayOf(schema)));
        } else {
          schemaFields.add(
              Schema.Field.of(field.getName(), Schema.arrayOf(Schema.of(getSchemaType(clazz)))));
        }
      } else {
        Schema schema = buildSchema(field.getName(), field.getType());
        schemaFields.add(Schema.Field.of(field.getName(), schema));
      }
    }
    return Schema.recordOf(schemaName, schemaFields);
  }

  /**
   * Returns selected list of Fields.
   *
   * @param clazz the class
   * @return The list of fields
   */
  public static List<Field> getModelFields(Class<?> clazz) {
    List<Field> fields = new ArrayList<>();
    Class currentClass = clazz;
    while (currentClass != Object.class) {
      Field[] declaredFields = currentClass.getDeclaredFields();
      fields.addAll(Arrays.asList(declaredFields));
      currentClass = currentClass.getSuperclass();
    }
    return fields;
  }

  private static Schema.Field getSimpleSchemaField(Field field) {
    return Schema.Field.of(
        field.getName(), Schema.nullableOf(Schema.of(getSchemaType(field.getType()))));
  }

  private static Schema.Type getSchemaType(Class clazz) {
    if (String.class.equals(clazz)) {
      return Schema.Type.STRING;
    } else if (Integer.class.equals(clazz)) {
      return Schema.Type.INT;
    } else if (Long.class.equals(clazz)) {
      return Schema.Type.LONG;
    } else if (Boolean.class.equals(clazz)) {
      return Schema.Type.BOOLEAN;
    } else if (List.class.equals(clazz)) {
      return Schema.Type.ARRAY;
    } else {
      return Schema.Type.RECORD;
    }
  }
}
