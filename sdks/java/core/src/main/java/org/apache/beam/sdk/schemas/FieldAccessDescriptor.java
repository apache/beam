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
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema.TypeName;

/**
 * Used inside of a {@link org.apache.beam.sdk.transforms.DoFn} to describe which fields in a schema
 * type need to be accessed for processing.
 */
public class FieldAccessDescriptor {
  private boolean allFields;
  private Set<Integer> fieldIdsAccessed;
  private Set<String> fieldNamesAccessed;
  private Map<Integer, FieldAccessDescriptor> nestedFieldsAccessedById;
  private Map<String, FieldAccessDescriptor> nestedFieldsAccessedByName;

  FieldAccessDescriptor(boolean allFields, Set<Integer> fieldsIdsAccessed,
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
  public static FieldAccessDescriptor allFields() {
    return new FieldAccessDescriptor(true, Collections.emptySet(), Collections.emptySet(),
        Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Return a descriptor that access the specified fields.
   */
  public static FieldAccessDescriptor fieldNames(String... names) {
    return fieldNames(Arrays.asList(names));
  }

  /**
   * Return a descriptor that access the specified fields.
   */
  public static FieldAccessDescriptor fieldNames(Iterable<String> fieldNames) {
    return new FieldAccessDescriptor(false, Collections.emptySet(),
        Sets.newHashSet(fieldNames), Collections.emptyMap(), Collections.emptyMap());
    //Iterable<Integer> ids = StreamSupport.stream(fieldNames.spliterator(), false)
    //    .map(n -> schema.indexOf(n))
   //     .collect(Collectors.toList());
  //  return fieldIds(ids);
  }

  /**
   * Return a descriptor that access the specified fields.
   */
  public FieldAccessDescriptor fieldIds(Integer... ids) {
    return fieldIds(Arrays.asList(ids));
  }

  /**
   * Return a descriptor that access the specified fields.
   */
  public FieldAccessDescriptor fieldIds(Iterable<Integer> ids) {
    return new FieldAccessDescriptor(false, Sets.newHashSet(ids),
        Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Return a descriptor that access the specified nested field. The nested field must be of type
   * {@link Schema.TypeName#ROW}, and the fieldAccess argument specifies what fields of the nested
   * type will be accessed.
   */
  public FieldAccessDescriptor withNestedField(
      String nestedFieldName, FieldAccessDescriptor fieldAccess) {
    return withNestedField(schema.indexOf(nestedFieldName), fieldAccess);
  }

  /**
   * Return a descriptor that access the specified nested field. The nested field must be of type
   * {@link Schema.TypeName#ROW}, and the fieldAccess argument specifies what fields of the nested
   * type will be accessed.
   */
  public FieldAccessDescriptor withNestedField(
      int nestedFieldId, FieldAccessDescriptor fieldAccess) {
    checkState(TypeName.ROW.equals(schema.getField(nestedFieldId).getType().getTypeName()));

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
    checkState(TypeName.ROW.equals(schema.getField(nestedFieldId).getType().getTypeName()));

    Map<String, FieldAccessDescriptor> newNestedFieldAccess =
        ImmutableMap.<String, FieldAccessDescriptor>builder()
            .putAll(nestedFieldsAccessedByName)
            .put(nestedFieldName, fieldAccess)
            .build();
    return new FieldAccessDescriptor(false, fieldIdsAccessed,
        fieldNamesAccessed, nestedFieldsAccessedById, newNestedFieldAccess);
  }

  public Iterable<Integer> resolveFieldIdsAccessed(Schema schema) {
    if (!fieldNamesAccessed.isEmpty()) {
      fieldIdsAccessed.addAll(
          StreamSupport.stream(fieldNamesAccessed.spliterator(), false)
              .map(n -> schema.indexOf(n))
              .collect(Collectors.toList()));
      // Once we've resolved all names into ids, no need to keep around the names.
      fieldNamesAccessed.clear();
    }

    return fieldIdsAccessed;
  }

  public Map<Integer, FieldAccessDescriptor> resolveNestedFieldsAccessed(Schema schema) {
    return nestedFieldsAccessed;
  }
}
