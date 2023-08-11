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
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A LogicalType representing a fixed-length string. */
public class FixedString extends PassThroughLogicalType<String> {
  public static final String IDENTIFIER =
      SchemaApi.LogicalTypes.Enum.FIXED_CHAR
          .getValueDescriptor()
          .getOptions()
          .getExtension(RunnerApi.beamUrn);
  private final @Nullable String name;
  private final int stringLength;

  /**
   * Return an instance of FixedString with specified string length.
   *
   * <p>The name, if set, refers to the TYPE name in the underlying database, for example, CHAR.
   */
  public static FixedString of(@Nullable String name, int stringLength) {
    return new FixedString(name, stringLength);
  }

  /** Return an instance of FixedString with specified string length. */
  public static FixedString of(int stringLength) {
    return new FixedString(null, stringLength);
  }

  private FixedString(@Nullable String name, int stringLength) {
    super(IDENTIFIER, FieldType.INT32, stringLength, FieldType.STRING);
    this.name = name;
    this.stringLength = stringLength;
  }

  public int getLength() {
    return stringLength;
  }

  public @Nullable String getName() {
    return name;
  }

  @Override
  public String toInputType(String base) {
    checkArgument(base.length() <= stringLength);

    return StringUtils.rightPad(base, stringLength);
  }

  @Override
  public String toString() {
    return "FixedString: " + stringLength;
  }
}
