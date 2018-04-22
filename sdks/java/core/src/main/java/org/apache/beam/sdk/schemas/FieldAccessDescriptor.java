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

import com.google.common.collect.ImmutableList;

/**
 * Used inside of a {@link org.apache.beam.sdk.transforms.DoFn} to describe which fields in a schema
 * type need to be accessed for processing.
 */
public class FieldAccessDescriptor {
  public static class FieldDescriptor {

  }
  // Return a descriptor that accesses all fields in a row.
  public static FieldAccessDescriptor allFields() {
    return new FieldAccessDescriptor();
  }

  /**
   * Return a descriptor that access the specified fields.
   */
  public static FieldAccessDescriptor fieldNames(String... names) {
    return new FieldAccessDescriptor();
  }

  /**
   * Return a descriptor that access the specified fields.
   */  public static FieldAccessDescriptor fieldNames(Iterable<String> fieldNames) {
    return new FieldAccessDescriptor();

  }

  /**
   * Return a descriptor that access the specified fields.
   */
  public static FieldAccessDescriptor fieldIds(int... ids) {
    return new FieldAccessDescriptor();
  }

  /**
   * Return a descriptor that access the specified fields.
   */
  public static FieldAccessDescriptor fieldIds(Iterable<Integer> ids) {
    return new FieldAccessDescriptor();
  }

  /**
   * Return a descriptor that access the specified nested field. The nested field must be of type
   * {@link Schema.TypeName#ROW}, and the fieldAccess argument specifies what fields of the nested
   * type will be accessed.
   */
  public static FieldAccessDescriptor withNestedField(
      String nestedFieldName, FieldAccessDescriptor fieldAccess) {
    return new FieldAccessDescriptor();
  }

  /**
   * Return a descriptor that access the specified nested field. The nested field must be of type
   * {@link Schema.TypeName#ROW}, and the fieldAccess argument specifies what fields of the nested
   * type will be accessed.
   */
  public static FieldAccessDescriptor withNestedField(
      int nestedFieldId, FieldAccessDescriptor fieldAccess) {
    return new FieldAccessDescriptor();
  }

  public Iterable<String> getFieldNamesAccessed() {
    return ImmutableList.of();
  }

  public Iterable<Integer> getFieldIdsAccessed() {
    return ImmutableList.of();
  }
}
