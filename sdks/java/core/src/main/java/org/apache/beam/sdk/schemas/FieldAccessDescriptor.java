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
  // Return a descriptor that accesses all fields in a row.
  public static FieldAccessDescriptor allFields() {
    return new FieldAccessDescriptor();
  }

  // Return a descriptor that accesses only the given set of fields in a row.
  public static FieldAccessDescriptor fromIterable(Iterable<String> fieldNames) {
    return new FieldAccessDescriptor();

  }

  public Iterable<String> getFieldsAccessed() {
    return ImmutableList.of();
  }

  // TODO: FieldAccessDescriptor currently assumes all fields are top level. Once we have native
  // support for nested schemas, we will need to boost this API to support addressing nested
  // fields (possibly using nested FieldAccessDescriptor objects).
}
