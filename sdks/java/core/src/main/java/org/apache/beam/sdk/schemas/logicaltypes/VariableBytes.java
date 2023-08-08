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

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A LogicalType representing a variable-length byte array with specified maximum length. */
public class VariableBytes extends PassThroughLogicalType<byte[]> {
  public static final String IDENTIFIER =
      SchemaApi.LogicalTypes.Enum.VAR_BYTES
          .getValueDescriptor()
          .getOptions()
          .getExtension(RunnerApi.beamUrn);
  private final @Nullable String name;
  private final int maxByteArrayLength;

  /**
   * Return an instance of VariableBytes with specified max byte array length.
   *
   * <p>The name, if set, refers to the TYPE name in the underlying database, for example, VARBINARY
   * and LONGVARBINARY.
   */
  public static VariableBytes of(@Nullable String name, int maxByteArrayLength) {
    return new VariableBytes(name, maxByteArrayLength);
  }

  /** Return an instance of VariableBytes with specified max byte array length. */
  public static VariableBytes of(int maxByteArrayLength) {
    return of(null, maxByteArrayLength);
  }

  private VariableBytes(@Nullable String name, int maxByteArrayLength) {
    super(IDENTIFIER, FieldType.INT32, maxByteArrayLength, FieldType.BYTES);
    this.name = name;
    this.maxByteArrayLength = maxByteArrayLength;
  }

  public int getMaxLength() {
    return maxByteArrayLength;
  }

  public @Nullable String getName() {
    return name;
  }

  @Override
  public byte[] toInputType(byte[] base) {
    checkArgument(base.length <= maxByteArrayLength);
    return base;
  }

  @Override
  public String toString() {
    return "VariableBytes: " + maxByteArrayLength;
  }
}
