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
  private Schema schema;
  private boolean allFields;
  private Set<Integer> fieldIdsAccessed;
  private Map<Integer, FieldAccessDescriptor> nestedFieldsAccessed;

  FieldAccessDescriptor(Schema schema, boolean allFields, Set<Integer> fieldsIdsAccessed,
                        Map<Integer, FieldAccessDescriptor> nestedFieldsAccessed) {
    this.schema = schema;
    this.allFields = allFields;
    this.fieldIdsAccessed = fieldsIdsAccessed;
    this.nestedFieldsAccessed = nestedFieldsAccessed;
  }

  public static FieldAccessDescriptor forSchema(Schema schema) {
    return new FieldAccessDescriptor(schema, false, Collections.emptySet(), Collections.emptyMap());
  }

  // Return a descriptor that accesses all fields in a row.
  public  FieldAccessDescriptor allFields() {
    return new FieldAccessDescriptor(schema, true, Collections.emptySet(),
        Collections.emptyMap());
  }

  /**
   * Return a descriptor that access the specified fields.
   */
  public FieldAccessDescriptor fieldNames(String... names) {
    return fieldNames(Arrays.asList(names));
  }

  /**
   * Return a descriptor that access the specified fields.
   */
  public FieldAccessDescriptor fieldNames(Iterable<String> fieldNames) {
    Iterable<Integer> ids = StreamSupport.stream(fieldNames.spliterator(), false)
        .map(n -> schema.indexOf(n))
        .collect(Collectors.toList());
    return fieldIds(ids);

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
    return new FieldAccessDescriptor(schema, false, Sets.newHashSet(ids), nestedFieldsAccessed);
  }

  /**
   * Return a descriptor that access the specified nested field. The nested field must be of type
   * {@link Schema.TypeName#ROW}, and the fieldAccess argument specifies what fields of the nested
   * type will be accessed.
   */
  public  FieldAccessDescriptor withNestedField(
      String nestedFieldName, FieldAccessDescriptor fieldAccess) {
    return withNestedField(schema.indexOf(nestedFieldName), fieldAccess);
  }

  /**
   * Return a descriptor that access the specified nested field. The nested field must be of type
   * {@link Schema.TypeName#ROW}, and the fieldAccess argument specifies what fields of the nested
   * type will be accessed.
   */
  public  FieldAccessDescriptor withNestedField(
      int nestedFieldId, FieldAccessDescriptor fieldAccess) {
    checkState(TypeName.ROW.equals(schema.getField(nestedFieldId).getType().getTypeName()));

    Map<Integer, FieldAccessDescriptor> newNestedFieldAccess =
        ImmutableMap.<Integer, FieldAccessDescriptor>builder()
        .putAll(nestedFieldsAccessed)
        .put(nestedFieldId, fieldAccess)
        .build();
    return new FieldAccessDescriptor(schema, false, fieldIdsAccessed, newNestedFieldAccess);
  }

  public Iterable<Integer> getFieldIdsAccessed() {
    return fieldIdsAccessed;
  }

  public Map<Integer, FieldAccessDescriptor> getNestedFieldsAccessed() {
    return nestedFieldsAccessed;
  }
}
