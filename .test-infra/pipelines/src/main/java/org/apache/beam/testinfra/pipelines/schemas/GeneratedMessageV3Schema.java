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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.NotImplementedException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class GeneratedMessageV3Schema implements SchemaProvider {

  @Override
  public @Nullable @UnknownKeyFor @Initialized <T> Schema schemaFor(
      @UnknownKeyFor @NonNull @Initialized TypeDescriptor<T> typeDescriptor) {
    checkState(GeneratedMessageV3.class.isAssignableFrom(typeDescriptor.getRawType()));
    GeneratedMessageV3Reflection<GeneratedMessageV3> reflection =
        new GeneratedMessageV3Reflection<>((Class<GeneratedMessageV3>) typeDescriptor.getRawType());
    GeneratedMessageV3SchemaBuilder<?> builder =
        new GeneratedMessageV3SchemaBuilder<>(Schema.builder(), reflection);
    return builder.build();
  }

  @Override
  public @Nullable @UnknownKeyFor @Initialized <T>
      SerializableFunction<T, @UnknownKeyFor @NonNull @Initialized Row> toRowFunction(
          @UnknownKeyFor @NonNull @Initialized TypeDescriptor<T> typeDescriptor) {
    checkState(GeneratedMessageV3.class.isAssignableFrom(typeDescriptor.getRawType()));
    return null;
  }

  @Override
  public @Nullable @UnknownKeyFor @Initialized <T>
      SerializableFunction<@UnknownKeyFor @NonNull @Initialized Row, T> fromRowFunction(
          @UnknownKeyFor @NonNull @Initialized TypeDescriptor<T> typeDescriptor) {
    checkState(GeneratedMessageV3.class.isAssignableFrom(typeDescriptor.getRawType()));
    throw new NotImplementedException("this feature is not needed for this pipeline");
  }
}
