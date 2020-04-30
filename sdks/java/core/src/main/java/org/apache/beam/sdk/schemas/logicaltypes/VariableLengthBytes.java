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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;

/** A LogicalType representing a variable-size byte array. */
@Experimental(Experimental.Kind.SCHEMAS)
public class VariableLengthBytes extends IdenticalBaseTAndInputTLogicalType<byte[]> {
  public static final String IDENTIFIER = "beam:logical_type:variable_length_bytes:v1";
  private final int byteArraySize;

  private VariableLengthBytes(int byteArraySize) {
    super(IDENTIFIER, Schema.FieldType.INT32, byteArraySize, Schema.FieldType.BYTES);
    this.byteArraySize = byteArraySize;
  }

  public static VariableLengthBytes of(int byteArraySize) {
    return new VariableLengthBytes(byteArraySize);
  }

  public int getLength() {
    return byteArraySize;
  }

  @Override
  public byte[] toBaseType(byte[] input) {
    checkArgument(input == null || input.length <= byteArraySize);
    return input;
  }

  @Override
  public byte[] toInputType(byte[] base) {
    checkArgument(base == null || base.length <= byteArraySize);
    return base;
  }
}
