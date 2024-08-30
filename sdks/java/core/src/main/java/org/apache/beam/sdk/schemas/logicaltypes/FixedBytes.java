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
package org.apache.beam.sdk.schemas.logicaltypes;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A LogicalType representing a fixed-length byte array. */
public class FixedBytes extends PassThroughLogicalType<byte[]> {
  public static final String IDENTIFIER =
      SchemaApi.LogicalTypes.Enum.FIXED_BYTES
          .getValueDescriptor()
          .getOptions()
          .getExtension(RunnerApi.beamUrn);

  private final @Nullable String name;
  private final int byteArrayLength;

  /**
   * Return an instance of FixedBytes with specified byte array length.
   *
   * <p>The name, if set, refers to the TYPE name in the underlying database, for example, BINARY.
   */
  public static FixedBytes of(@Nullable String name, int byteArrayLength) {
    return new FixedBytes(name, byteArrayLength);
  }

  /** Return an instance of FixedBytes with specified byte array length. */
  public static FixedBytes of(int byteArrayLength) {
    return of(null, byteArrayLength);
  }

  private FixedBytes(@Nullable String name, int byteArrayLength) {
    super(IDENTIFIER, FieldType.INT32, byteArrayLength, FieldType.BYTES);
    this.name = name;
    this.byteArrayLength = byteArrayLength;
  }

  public int getLength() {
    return byteArrayLength;
  }

  public @Nullable String getName() {
    return name;
  }

  @Override
  public byte[] toBaseType(byte[] input) {
    checkArgument(input.length == byteArrayLength);
    return input;
  }

  @Override
  public byte[] toInputType(byte[] base) {
    checkArgument(base.length <= byteArrayLength);
    if (base.length == byteArrayLength) {
      return base;
    } else {
      return Arrays.copyOf(base, byteArrayLength);
    }
  }

  @Override
  public String toString() {
    return "FixedBytes: " + byteArrayLength;
  }
}
