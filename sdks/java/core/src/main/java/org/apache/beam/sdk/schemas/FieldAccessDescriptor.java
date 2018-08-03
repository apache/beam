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

import com.google.auto.value.AutoValue;
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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;

/**
 * Used inside of a {@link org.apache.beam.sdk.transforms.DoFn} to describe which fields in a schema
 * type need to be accessed for processing.
 */
@Experimental(Kind.SCHEMAS)
@AutoValue
public abstract class FieldAccessDescriptor implements Serializable {
  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setAllFields(boolean allFields);

    abstract Builder setFieldIdsAccessed(Set<Integer> fieldIdsAccessed);

    abstract Builder setFieldNamesAccessed(Set<String> fieldNamesAccessed);

    abstract Builder setNestedFieldsAccessedById(
        Map<Integer, FieldAccessDescriptor> nestedFieldsAccessedById);

    abstract Builder setNestedFieldsAccessedByName(
        Map<String, FieldAccessDescriptor> nestedFieldsAccessedByName);

    abstract FieldAccessDescriptor build();
  }

  abstract boolean getAllFields();

  abstract Set<Integer> getFieldIdsAccessed();

  abstract Set<String> getFieldNamesAccessed();

  abstract Map<Integer, FieldAccessDescriptor> getNestedFieldsAccessedById();

  abstract Map<String, FieldAccessDescriptor> getNestedFieldsAccessedByName();

  abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_FieldAccessDescriptor.Builder()
        .setAllFields(false)
        .setFieldIdsAccessed(Collections.emptySet())
        .setFieldNamesAccessed(Collections.emptySet())
        .setNestedFieldsAccessedById(Collections.emptyMap())
        .setNestedFieldsAccessedByName(Collections.emptyMap());
  }

  // Return a descriptor that accesses all fields in a row.
  public static FieldAccessDescriptor withAllFields() {
    return builder().setAllFields(true).build();
  }

  /**
   * Return a descriptor that access the specified fields.
   *
   * <p>By default, if the field is a nested row (or a container containing a row), all fields of
   * said rows are accessed. For finer-grained acccess to nested rows, call withNestedField and pass
   * in a recursive {@link FieldAccessDescriptor}.
   */
  public static FieldAccessDescriptor withFieldNames(String... names) {
    return withFieldNames(Arrays.asList(names));
  }

  /**
   * Return a descriptor that access the specified fields.
   *
   * <p>By default, if the field is a nested row (or a container containing a row), all fields of
   * said rows are accessed. For finer-grained acccess to nested rows, call withNestedField and pass
   * in a recursive {@link FieldAccessDescriptor}.
   */
  public static FieldAccessDescriptor withFieldNames(Iterable<String> fieldNames) {
    return builder().setFieldNamesAccessed(Sets.newTreeSet(fieldNames)).build();
  }

  /**
   * Return a descriptor that access the specified fields.
   *
   * <p>By default, if the field is a nested row (or a container containing a row), all fields of
   * said rows are accessed. For finer-grained acccess to nested rows, call withNestedField and pass
   * in a recursive {@link FieldAccessDescriptor}.
   */
  public static FieldAccessDescriptor withFieldIds(Integer... ids) {
    return withFieldIds(Arrays.asList(ids));
  }

  /**
   * Return a descriptor that access the specified fields.
   *
   * <p>By default, if the field is a nested row (or a container containing a row), all fields of
   * said rows are accessed. For finer-grained acccess to nested rows, call withNestedField and pass
   * in a recursive {@link FieldAccessDescriptor}.
   */
  public static FieldAccessDescriptor withFieldIds(Iterable<Integer> ids) {
    return builder().setFieldIdsAccessed(Sets.newTreeSet(ids)).build();
  }

  /** Return an empty {@link FieldAccessDescriptor}. */
  public static FieldAccessDescriptor create() {
    return builder().build();
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
            .putAll(getNestedFieldsAccessedById())
            .put(nestedFieldId, fieldAccess)
            .build();
    return toBuilder().setNestedFieldsAccessedById(newNestedFieldAccess).build();
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
            .putAll(getNestedFieldsAccessedByName())
            .put(nestedFieldName, fieldAccess)
            .build();
    return toBuilder().setNestedFieldsAccessedByName(newNestedFieldAccess).build();
  }

  public boolean allFields() {
    return getAllFields();
  }

  public Set<Integer> fieldIdsAccessed() {
    return getFieldIdsAccessed();
  }

  public Map<Integer, FieldAccessDescriptor> nestedFields() {
    return getNestedFieldsAccessedById();
  }

  public FieldAccessDescriptor resolve(Schema schema) {
    Set<Integer> resolvedFieldIdsAccessed = resolveFieldIdsAccessed(schema);
    Map<Integer, FieldAccessDescriptor> resolvedNestedFieldsAccessed =
        resolveNestedFieldsAccessed(schema);

    checkState(
        !getAllFields() || resolvedNestedFieldsAccessed.isEmpty(),
        "nested fields cannot be set if allFields is also set");

    // If a recursive access is set for any nested fields, remove those fields from
    // fieldIdsAccessed.
    resolvedFieldIdsAccessed.removeAll(resolvedNestedFieldsAccessed.keySet());

    return builder()
        .setAllFields(getAllFields())
        .setFieldIdsAccessed(resolvedFieldIdsAccessed)
        .setNestedFieldsAccessedById(resolvedNestedFieldsAccessed)
        .build();
  }

  private Set<Integer> resolveFieldIdsAccessed(Schema schema) {
    Set<Integer> fieldIds = Sets.newTreeSet();
    for (int fieldId : getFieldIdsAccessed()) {
      fieldIds.add(validateFieldId(schema, fieldId));
    }
    if (!getFieldNamesAccessed().isEmpty()) {
      fieldIds.addAll(
          StreamSupport.stream(getFieldNamesAccessed().spliterator(), false)
              .map(name -> schema.indexOf(name))
              .collect(Collectors.toList()));
    }
    return fieldIds;
  }

  private static Schema getFieldSchema(Field field) {
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
      throw new IllegalArgumentException(
          "Field " + field + " must be either a row or " + " a container containing rows");
    }
  }

  private FieldAccessDescriptor resolvedNestedFieldsHelper(
      Field field, FieldAccessDescriptor subDescriptor) {
    return subDescriptor.resolve(getFieldSchema(field));
  }

  private Map<Integer, FieldAccessDescriptor> resolveNestedFieldsAccessed(Schema schema) {
    Map<Integer, FieldAccessDescriptor> nestedFields = Maps.newTreeMap();

    nestedFields.putAll(
        getNestedFieldsAccessedByName()
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    e -> schema.indexOf(e.getKey()),
                    e -> resolvedNestedFieldsHelper(schema.getField(e.getKey()), e.getValue()))));
    nestedFields.putAll(
        getNestedFieldsAccessedById()
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    e -> validateFieldId(schema, e.getKey()),
                    e -> resolvedNestedFieldsHelper(schema.getField(e.getKey()), e.getValue()))));

    return nestedFields;
  }

  private static int validateFieldId(Schema schema, int fieldId) {
    if (fieldId < 0 || fieldId >= schema.getFieldCount()) {
      throw new IllegalArgumentException("Invalid field id " + fieldId + " for schema " + schema);
    }
    return fieldId;
  }
}
