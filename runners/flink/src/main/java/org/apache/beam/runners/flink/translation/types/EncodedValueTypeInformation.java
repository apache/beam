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
package org.apache.beam.runners.flink.translation.types;

import org.apache.beam.sdk.coders.Coder;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Flink {@link TypeInformation} for Beam values that have been encoded to byte data by a {@link
 * Coder}.
 */
public class EncodedValueTypeInformation extends TypeInformation<byte[]>
    implements AtomicType<byte[]> {

  private static final long serialVersionUID = 1L;

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 0;
  }

  @Override
  public int getTotalFields() {
    return 0;
  }

  @Override
  public Class<byte[]> getTypeClass() {
    return byte[].class;
  }

  @Override
  public boolean isKeyType() {
    return true;
  }

  @Override
  public TypeSerializer<byte[]> createSerializer(ExecutionConfig executionConfig) {
    return new EncodedValueSerializer();
  }

  @Override
  public boolean equals(@Nullable Object other) {
    return other instanceof EncodedValueTypeInformation;
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode();
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof EncodedValueTypeInformation;
  }

  @Override
  public String toString() {
    return "EncodedValueTypeInformation";
  }

  @Override
  public TypeComparator<byte[]> createComparator(
      boolean sortOrderAscending, ExecutionConfig executionConfig) {
    return new EncodedValueComparator(sortOrderAscending);
  }
}
