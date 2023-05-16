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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.GeneratedMessageV3;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;

class GeneratedMessageV3Reflection<T extends GeneratedMessageV3> {

  private static final String GET_DEFAULT_INSTANCE_METHOD_NAME = "getDefaultInstance";
  private static final String GET_DESCRIPTOR_FOR_TYPE_METHOD_NAME = "getDescriptorForType";

  private final TypeDescriptor<T> type;

  private final Class<T> clazz;

  private @MonotonicNonNull T defaultInstance;
  private @MonotonicNonNull Descriptor descriptorForType;

  private @MonotonicNonNull DescriptorReflection descriptorReflection;

  GeneratedMessageV3Reflection(Class<T> clazz) {
    this.type = TypeDescriptor.of(clazz);
    this.clazz = clazz;
  }

  @NonNull
  DescriptorReflection getDescriptorReflection() {
    if (descriptorReflection == null) {
      descriptorReflection = new DescriptorReflection(getDescriptorForType());
    }
    return checkStateNotNull(descriptorReflection);
  }

  @NonNull
  Map<String, FieldDescriptor> getFieldDescriptorMap() {
    return getDescriptorReflection().getFieldDescriptorMap();
  }

  @NonNull
  Collection<FieldDescriptor> getFields() {
    return getFieldDescriptorMap().values();
  }

  @NonNull
  List<FieldDescriptor> getNonRepeatedFieldsForType(JavaType type) {
    return FieldDescriptorFilter.of(getFields()).ofType(type).nonRepeated().getFields();
  }

  @NonNull
  List<FieldDescriptor> getRepeatedFieldsForType(JavaType type) {
    return FieldDescriptorFilter.of(getFields()).ofType(type).repeated().getFields();
  }

  @NonNull
  List<FieldDescriptor> getNonRepeatedEnumTypes() {
    return FieldDescriptorFilter.of(getFields()).nonRepeated().enums().nonRepeated().getFields();
  }

  @NonNull
  List<FieldDescriptor> getRepeatedEnumTypes() {
    return FieldDescriptorFilter.of(getFields()).repeated().enums().nonRepeated().getFields();
  }

  @NonNull
  List<FieldDescriptor> getNonRepeatedExtensions() {
    return FieldDescriptorFilter.of(getFields()).nonRepeated().extended().getFields();
  }

  @NonNull
  List<FieldDescriptor> getRepeatedExtensions() {
    return FieldDescriptorFilter.of(getFields()).repeated().extended().getFields();
  }

  @NonNull
  List<FieldDescriptor> getNonRepeatedMapTypes() {
    return FieldDescriptorFilter.of(getFields()).maps().getFields();
  }

  @NonNull
  T getDefaultInstance() {
    if (defaultInstance == null) {
      try {
        Object obj = getDefaultInstanceMethod().invoke(type.getRawType());
        checkState(type.getRawType().isInstance(obj));
        defaultInstance = checkStateNotNull(clazz.cast(obj));
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    }
    return checkStateNotNull(defaultInstance);
  }

  @NonNull
  Descriptor getDescriptorForType() {
    if (descriptorForType == null) {
      try {
        descriptorForType =
            checkStateNotNull(
                (Descriptor) getDescriptorForTypeMethod().invoke(getDefaultInstance()));
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    }
    return checkStateNotNull(descriptorForType);
  }

  @NonNull
  Method getDescriptorForTypeMethod() {
    try {
      return type.getRawType().getMethod(GET_DESCRIPTOR_FOR_TYPE_METHOD_NAME);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(e);
    }
  }

  @NonNull
  Method getDefaultInstanceMethod() {
    try {
      return type.getRawType().getMethod(GET_DEFAULT_INSTANCE_METHOD_NAME);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(e);
    }
  }
}
