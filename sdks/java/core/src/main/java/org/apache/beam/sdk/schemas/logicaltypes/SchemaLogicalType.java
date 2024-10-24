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

import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A schema represented as a serialized proto bytes. */
public class SchemaLogicalType implements Schema.LogicalType<Schema, byte[]> {
  public static final String IDENTIFIER = "beam:logical_type:schema:v1";

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public @Nullable FieldType getArgumentType() {
    return null;
  }

  @Override
  public FieldType getBaseType() {
    return FieldType.BYTES;
  }

  @Override
  public byte @NonNull [] toBaseType(Schema input) {
    return SchemaTranslation.schemaToProto(input, true).toByteArray();
  }

  @Override
  public org.apache.beam.sdk.schemas.Schema toInputType(byte @NonNull [] base) {
    try {
      return SchemaTranslation.schemaFromProto(SchemaApi.Schema.parseFrom(base));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
