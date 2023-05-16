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
import com.google.protobuf.Descriptors.Descriptor;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;

class DescriptorReflection {
  private final Descriptor descriptor;
  private final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap = new HashMap<>();

  DescriptorReflection(Descriptor descriptor) {
    this.descriptor = descriptor;
  }

  @NonNull
  Map<String, Descriptors.FieldDescriptor> getFieldDescriptorMap() {
    if (fieldDescriptorMap.isEmpty()) {
      for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
        fieldDescriptorMap.put(field.getName(), field);
      }
    }
    return fieldDescriptorMap;
  }

  Descriptors.@NonNull FieldDescriptor getField(String name) {
    return checkStateNotNull(getFieldDescriptorMap().get(name));
  }

  @NonNull
  Collection<Descriptors.FieldDescriptor> getFields() {
    return getFieldDescriptorMap().values();
  }

  @NonNull
  String getName() {
    return descriptor.getName();
  }

  @NonNull
  String getFullName() {
    return descriptor.getFullName();
  }
}
