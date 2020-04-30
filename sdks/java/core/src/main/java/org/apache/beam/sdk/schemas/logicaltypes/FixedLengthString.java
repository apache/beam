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

/** A LogicalType representing a fixed-length string. */
@Experimental(Experimental.Kind.SCHEMAS)
public class FixedLengthString extends IdenticalBaseTAndInputTLogicalType<String> {
  public static final String IDENTIFIER = "beam:logical_type:fixed_length_string:v1";
  private final int length;

  private FixedLengthString(int length) {
    super(IDENTIFIER, Schema.FieldType.INT32, length, Schema.FieldType.STRING);
    this.length = length;
  }

  public int getLength() {
    return length;
  }

  public static FixedLengthString of(int length) {
    return new FixedLengthString(length);
  }

  @Override
  public String toBaseType(String input) {
    checkArgument(input == null || input.length() == length);
    return input;
  }

  @Override
  public String toInputType(String base) {
    checkArgument(base == null || base.length() == length);
    return base;
  }
}
