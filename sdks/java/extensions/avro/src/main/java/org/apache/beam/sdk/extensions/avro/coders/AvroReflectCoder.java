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
package org.apache.beam.sdk.extensions.avro.coders;

import org.apache.avro.Schema;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.Union;
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * AvroCoder specialisation for avro classes using Java reflection.
 *
 * <p>Only concrete classes with a no-argument constructor can be mapped to Avro records. All
 * inherited fields that are not static or transient are included. Fields are not permitted to be
 * null unless annotated by {@link Nullable} or a {@link Union} schema containing {@code "null"}.
 */
public class AvroReflectCoder<T> extends AvroCoder<T> {

  @SuppressWarnings("nullness") // new ReflectData(ClassLoader) is not annotated to accept null
  AvroReflectCoder(Class<T> type) {
    super(
        type,
        AvroDatumFactory.ReflectDatumFactory.of(type),
        new ReflectData(type.getClassLoader()).getSchema(type));
  }

  AvroReflectCoder(Class<T> type, Schema schema) {
    super(type, AvroDatumFactory.ReflectDatumFactory.of(type), schema);
  }

  public static <T> AvroReflectCoder<T> of(TypeDescriptor<T> type) {
    return new AvroReflectCoder<>((Class<T>) type.getRawType());
  }

  public static <T> AvroReflectCoder<T> of(Class<T> type) {
    return new AvroReflectCoder<>(type);
  }

  public static <T> AvroReflectCoder<T> of(Class<T> type, Schema schema) {
    return new AvroReflectCoder<>(type, schema);
  }
}
