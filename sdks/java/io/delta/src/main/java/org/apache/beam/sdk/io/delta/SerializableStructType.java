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
package org.apache.beam.sdk.io.delta;

import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.types.StructType;
import java.io.Serializable;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A serializable wrapper for {@link StructType} using the Delta Kernel JSON
 * serializer/deserializer.
 */
public class SerializableStructType implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String jsonSchema;
  private transient StructType structType;

  public SerializableStructType(StructType structType) {
    this.structType = Objects.requireNonNull(structType, "structType cannot be null");
    this.jsonSchema = DataTypeJsonSerDe.serializeStructType(structType);
  }

  public StructType get() {
    if (structType == null) {
      structType = DataTypeJsonSerDe.deserializeStructType(jsonSchema);
    }
    return structType;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SerializableStructType)) {
      return false;
    }
    SerializableStructType that = (SerializableStructType) o;
    return Objects.equals(jsonSchema, that.jsonSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jsonSchema);
  }

  @Override
  public String toString() {
    return jsonSchema;
  }
}
