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

/** A LogicalType representing a variable-length string with specified maximum length. */
public class VariableString extends PassThroughLogicalType<String> {
  public static final String IDENTIFIER =
      SchemaApi.LogicalTypes.Enum.VAR_CHAR
          .getValueDescriptor()
          .getOptions()
          .getExtension(RunnerApi.beamUrn);
  private final @Nullable String name;
  private final int maxStringLength;

  /**
   * Return an instance of VariableString with specified max string length.
   *
   * <p>The name, if set, refers to the TYPE name in the underlying database, for example, VARCHAR
   * and LONGVARCHAR.
   */
  public static VariableString of(@Nullable String name, int maxStringLength) {
    return new VariableString(name, maxStringLength);
  }

  /** Return an instance of VariableString with specified max string length. */
  public static VariableString of(int maxStringLength) {
    return of(null, maxStringLength);
  }

  private VariableString(@Nullable String name, int maxStringLength) {
    super(IDENTIFIER, FieldType.INT32, maxStringLength, FieldType.STRING);
    this.name = name;
    this.maxStringLength = maxStringLength;
  }

  public int getMaxLength() {
    return maxStringLength;
  }

  public @Nullable String getName() {
    return name;
  }

  @Override
  public String toInputType(String base) {
    checkArgument(base.length() <= maxStringLength);
    return base;
  }

  @Override
  public String toString() {
    return "VariableString: " + maxStringLength;
  }
}
