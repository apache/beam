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

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

class FieldDescriptorFilter<T extends FieldDescriptor> {

  static <T extends FieldDescriptor> FieldDescriptorFilter<T> of(@NonNull List<T> fields) {
    return new FieldDescriptorFilter<>(fields);
  }

  static <T extends FieldDescriptor> FieldDescriptorFilter<T> of(@NonNull Collection<T> fields) {
    return new FieldDescriptorFilter<>(new ArrayList<>(fields));
  }

  private final List<T> fields;

  FieldDescriptorFilter(List<T> fields) {
    this.fields = fields;
  }

  public List<T> getFields() {
    return fields;
  }

  FieldDescriptorFilter<T> enums() {
    return new FieldDescriptorFilter<>(
        fields.stream()
            .filter(field -> field.getJavaType().equals(JavaType.ENUM))
            .collect(Collectors.toList()));
  }

  FieldDescriptorFilter<T> extended() {
    return new FieldDescriptorFilter<>(
        fields.stream().filter(FieldDescriptor::isExtension).collect(Collectors.toList()));
  }

  FieldDescriptorFilter<T> repeated() {
    return new FieldDescriptorFilter<>(
        fields.stream().filter(FieldDescriptor::isRepeated).collect(Collectors.toList()));
  }

  FieldDescriptorFilter<T> maps() {
    return new FieldDescriptorFilter<>(
        fields.stream().filter(FieldDescriptor::isMapField).collect(Collectors.toList()));
  }

  FieldDescriptorFilter<T> nonRepeated() {
    return new FieldDescriptorFilter<>(
        fields.stream().filter(field -> !field.isRepeated()).collect(Collectors.toList()));
  }

  FieldDescriptorFilter<T> ofType(JavaType type) {
    return new FieldDescriptorFilter<>(
        fields.stream()
            .filter(field -> field.getJavaType().equals(type))
            .collect(Collectors.toList()));
  }
}
