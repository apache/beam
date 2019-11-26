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
package org.apache.beam.sdk.schemas;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;

/** A collection of common {@link Schema.LogicalType} classes. */
public class LogicalTypes {
  /** A base class for LogicalTypes that use the same Java type as the underlying base type. */
  public abstract static class PassThroughLogicalType<T> implements LogicalType<T, T> {
    private final String identifier;
    private final String argument;
    private final FieldType fieldType;

    protected PassThroughLogicalType(String identifier, String argument, FieldType fieldType) {
      this.identifier = identifier;
      this.argument = argument;
      this.fieldType = fieldType;
    }

    @Override
    public String getIdentifier() {
      return identifier;
    }

    @Override
    public String getArgument() {
      return argument;
    }

    @Override
    public FieldType getBaseType() {
      return fieldType;
    }

    @Override
    public T toBaseType(T input) {
      return input;
    }

    @Override
    public T toInputType(T base) {
      return base;
    }
  }

  /** A LogicalType representing a fixed-size byte array. */
  public static class FixedBytes implements LogicalType<byte[], byte[]> {
    public static final String IDENTIFIER = "FixedBytes";
    private final int byteArraySize;

    private FixedBytes(int byteArraySize) {
      this.byteArraySize = byteArraySize;
    }

    public static FixedBytes of(int byteArraySize) {
      return new FixedBytes(byteArraySize);
    }

    public int getLength() {
      return byteArraySize;
    }

    @Override
    public String getIdentifier() {
      return IDENTIFIER;
    }

    @Override
    public String getArgument() {
      return Integer.toString(byteArraySize);
    }

    @Override
    public FieldType getBaseType() {
      return FieldType.BYTES;
    }

    @Override
    public byte[] toBaseType(byte[] input) {
      checkArgument(input.length == byteArraySize);
      return input;
    }

    @Override
    public byte[] toInputType(byte[] base) {
      checkArgument(base.length <= byteArraySize);
      if (base.length == byteArraySize) {
        return base;
      } else {
        return Arrays.copyOf(base, byteArraySize);
      }
    }
  }
}
