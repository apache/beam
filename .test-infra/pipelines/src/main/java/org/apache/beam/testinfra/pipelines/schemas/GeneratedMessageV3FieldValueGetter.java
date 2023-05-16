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
package org.apache.beam.testinfra.pipelines.schemas;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

class GeneratedMessageV3FieldValueGetter<ObjectT extends GeneratedMessageV3, ValueT>
    implements FieldValueGetter<ObjectT, ValueT> {

  private final Descriptors.FieldDescriptor fieldDescriptor;

  GeneratedMessageV3FieldValueGetter(Descriptors.FieldDescriptor fieldDescriptor) {
    this.fieldDescriptor = fieldDescriptor;
  }

  @Override
  public @Nullable ValueT get(ObjectT object) {
    return (ValueT) checkStateNotNull(object.getAllFields().get(fieldDescriptor));
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String name() {
    return fieldDescriptor.getName();
  }
}
