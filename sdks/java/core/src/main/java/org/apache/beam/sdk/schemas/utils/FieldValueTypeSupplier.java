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
package org.apache.beam.sdk.schemas.utils;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A naming policy for schema fields. This maps a name from the class (field name or getter name) to
 * the matching field name in the schema.
 */
public interface FieldValueTypeSupplier extends Serializable {
  /** Return all the FieldValueTypeInformations. */
  List<FieldValueTypeInformation> get(TypeDescriptor<?> typeDescriptor);

  /**
   * Return all the FieldValueTypeInformations.
   *
   * <p>If the schema parameter is not null, then the returned list must be in the same order as
   * fields in the schema.
   */
  default List<FieldValueTypeInformation> get(TypeDescriptor<?> typeDescriptor, Schema schema) {
    return StaticSchemaInference.sortBySchema(get(typeDescriptor), schema);
  }
}
