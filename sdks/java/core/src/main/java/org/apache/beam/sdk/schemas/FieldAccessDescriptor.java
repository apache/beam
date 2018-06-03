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


package org.apache.beam.sdk.schemas;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;

/**
 * Used inside of a {@link org.apache.beam.sdk.transforms.DoFn} to describe which fields in a schema
 * type need to be accessed for processing.
 */
public class FieldAccessDescriptor implements Serializable {
  private boolean allFields;
  private Set<Integer> fieldIdsAccessed;
  private Set<String> fieldNamesAccessed;
  private Map<Integer, FieldAccessDescriptor> nestedFieldsAccessedById;
  private Map<String, FieldAccessDescriptor> nestedFieldsAccessedByName;

  FieldAccessDescriptor(boolean allFields,
                        Set<Integer> fieldsIdsAccessed,
                        Set<String> fieldNamesAccessed,
                        Map<Integer, FieldAccessDescriptor> nestedFieldsAccessedById,
                        Map<String, FieldAccessDescriptor> nestedFieldsAccessedByName) {
    this.allFields = allFields;
    this.fieldIdsAccessed = fieldsIdsAccessed;
    this.fieldNamesAccessed = fieldNamesAccessed;
    this.nestedFieldsAccessedById = nestedFieldsAccessedById;
    this.nestedFieldsAccessedByName = nestedFieldsAccessedByName;
  }

  // Return a descriptor that accesses all fields in a row.
  public static FieldAccessDescriptor withAllFields() {
    return new FieldAccessDescriptor(true, Collections.emptySet(), Collections.emptySet(),
        Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Return a descriptor that access the specified fields.
   *
   * <p>By default, if the field is a nested row (or a container containing a row), all fields of
   * said rows are accessed. For finer-grained acccess to nested rows, call withNestedField and
   * pass in a recursive {@link FieldAccessDescriptor}.
   */
  public static FieldAccessDescriptor withFieldNames(String... names) {
    return withFieldNames(Arrays.asList(names));
  }

  /**
   * Return a descriptor that access the specified fields.
   *
   * <p>By default, if the field is a nested row (or a container containing a row), all fields of
   * said rows are accessed. For finer-grained acccess to nested rows, call withNestedField and
   * pass in a recursive {@link FieldAccessDescriptor}.
   */
  public static FieldAccessDescriptor withFieldNames(Iterable<String> fieldNames) {
    return new FieldAccessDescriptor(false, Collections.emptySet(),
        Sets.newHashSet(fieldNames), Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Return a descriptor that access the specified fields.
   *
   * <p>By default, if the field is a nested row (or a container containing a row), all fields of
   * said rows are accessed. For finer-grained acccess to nested rows, call withNestedField and
   * pass in a recursive {@link FieldAccessDescriptor}.
   */
  public static FieldAccessDescriptor withFieldIds(Integer... ids) {
    return withFieldIds(Arrays.asList(ids));
  }

  /**
   * Return a descriptor that access the specified fields.
   *
   * <p>By default, if the field is a nested row (or a container containing a row), all fields of
   * said rows are accessed. For finer-grained acccess to nested rows, call withNestedField and
   * pass in a recursive {@link FieldAccessDescriptor}.
   */
  public static FieldAccessDescriptor withFieldIds(Iterable<Integer> ids) {
    return new FieldAccessDescriptor(false, Sets.newHashSet(ids),
        Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap());
  }


  /**
   * Return a descriptor that access the specified nested field. The nested field must be of type
   * {@link Schema.TypeName#ROW}, and the fieldAccess argument specifies what fields of the nested
   * type will be accessed.
   */
  public FieldAccessDescriptor withNestedField(
      int nestedFieldId, FieldAccessDescriptor fieldAccess) {
    Map<Integer, FieldAccessDescriptor> newNestedFieldAccess =
        ImmutableMap.<Integer, FieldAccessDescriptor>builder()
        .putAll(nestedFieldsAccessedById)
        .put(nestedFieldId, fieldAccess)
        .build();
    return new FieldAccessDescriptor(false, fieldIdsAccessed,
        fieldNamesAccessed, newNestedFieldAccess, nestedFieldsAccessedByName);
  }

  /**
   * Return a descriptor that access the specified nested field. The nested field must be of type
   * {@link Schema.TypeName#ROW}, and the fieldAccess argument specifies what fields of the nested
   * type will be accessed.
   */
  public FieldAccessDescriptor withNestedField(
      String nestedFieldName, FieldAccessDescriptor fieldAccess) {
    Map<String, FieldAccessDescriptor> newNestedFieldAccess =
        ImmutableMap.<String, FieldAccessDescriptor>builder()
            .putAll(nestedFieldsAccessedByName)
            .put(nestedFieldName, fieldAccess)
            .build();
    return new FieldAccessDescriptor(false, fieldIdsAccessed,
        fieldNamesAccessed, nestedFieldsAccessedById, newNestedFieldAccess);
  }

  public boolean allFields() {
    return allFields;
  }

  public Set<Integer> fieldIdsAccessed() {
    return fieldIdsAccessed;
  }

  public Map<Integer, FieldAccessDescriptor> nestedFields() {
    return nestedFieldsAccessedById;
  }

  public FieldAccessDescriptor resolve(Schema schema) {
    Set<Integer> fieldIdsAccessed = resolveFieldIdsAccessed(schema);
    Map<Integer, FieldAccessDescriptor> nestedFieldsAccessed = resolveNestedFieldsAccessed(schema);

    checkState(!allFields || nestedFieldsAccessed.isEmpty(),
    "nested fields cannot be set if allFields is also set");

    // If a recursive access is set for any nested fields, remove those fields from
    // fieldIdsAccessed.
    fieldIdsAccessed.removeAll(nestedFieldsAccessed.keySet());

    return new FieldAccessDescriptor(this.allFields,
        fieldIdsAccessed,
        Collections.emptySet(),
        nestedFieldsAccessed,
        Collections.emptyMap());
  }

  private Set<Integer> resolveFieldIdsAccessed(Schema schema) {
    Set<Integer> fieldsIds = Sets.newHashSet(fieldIdsAccessed);
    if (!fieldNamesAccessed.isEmpty()) {
      fieldsIds.addAll(
          StreamSupport.stream(fieldNamesAccessed.spliterator(), false)
              .map(name -> schema.indexOf(name))
              .collect(Collectors.toList()));
    }
    return fieldsIds;
  }

  private Schema getFieldSchema(Field field) {
    FieldType type = field.getType();
    if (TypeName.ROW.equals(type.getTypeName())) {
      return type.getRowSchema();
    } else if (TypeName.ARRAY.equals(type.getTypeName())
        && TypeName.ROW.equals(type.getCollectionElementType().getTypeName())) {
      return type.getCollectionElementType().getRowSchema();
    } else if (TypeName.MAP.equals(type.getTypeName())
      && TypeName.ROW.equals(type.getMapValueType().getTypeName())) {
      return type.getMapValueType().getRowSchema();
    } else {
      checkState(false, "Field " + field + " must be either a row or "
      + " a container containing rows");
    }
    return null;
  }

  private FieldAccessDescriptor resolvedNestedFieldsHelper(Field field,
                                                           FieldAccessDescriptor subDescriptor) {
    return subDescriptor.resolve(getFieldSchema(field));
  }

  private Map<Integer, FieldAccessDescriptor> resolveNestedFieldsAccessed(Schema schema) {
    Map<Integer, FieldAccessDescriptor> nestedFields = Maps.newHashMap();

    nestedFields.putAll(
        nestedFieldsAccessedByName.entrySet().stream()
        .collect(Collectors.toMap(
            e -> schema.indexOf(e.getKey()),
            e -> resolvedNestedFieldsHelper(schema.getField(e.getKey()), e.getValue()))));
    nestedFields.putAll(
        nestedFieldsAccessedById.entrySet().stream()
        .collect(Collectors.toMap(
            e -> e.getKey(),
            e -> resolvedNestedFieldsHelper(schema.getField(e.getKey()), e.getValue()))));

    return nestedFields;
  }
}
