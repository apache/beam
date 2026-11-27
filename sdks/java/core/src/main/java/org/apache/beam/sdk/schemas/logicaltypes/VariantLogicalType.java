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

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.variant.Variant;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A portable logical type for semi-structured data ({@code beam:logical_type:variant:v1}).
 *
 * <p>The Java value is a {@link Variant}. The representation type is {@code ROW<metadata: BYTES,
 * value: BYTES>} holding the <a
 * href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">Parquet Variant
 * binary encoding</a>, the same bytes used by Apache Iceberg v3, Apache Spark and Apache Flink.
 * SDKs that do not know this URN see a plain row with two byte fields.
 *
 * <p>Use {@link SqlTypes#VARIANT} to refer to this type in a schema:
 *
 * <pre>{@code
 * Schema schema = Schema.builder()
 *     .addInt64Field("id")
 *     .addLogicalTypeField("payload", SqlTypes.VARIANT)
 *     .build();
 * }</pre>
 */
public class VariantLogicalType implements Schema.LogicalType<Variant, Row> {
  public static final String IDENTIFIER =
      SchemaApi.LogicalTypes.Enum.VARIANT
          .getValueDescriptor()
          .getOptions()
          .getExtension(RunnerApi.beamUrn);

  public static final String METADATA_FIELD = "metadata";
  public static final String VALUE_FIELD = "value";

  /** The representation (base) schema: {@code ROW<metadata: BYTES, value: BYTES>}. */
  public static final Schema BASE_SCHEMA =
      Schema.builder().addByteArrayField(METADATA_FIELD).addByteArrayField(VALUE_FIELD).build();

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Schema.@Nullable FieldType getArgumentType() {
    return null;
  }

  @Override
  public Schema.FieldType getBaseType() {
    return Schema.FieldType.row(BASE_SCHEMA);
  }

  @Override
  public Row toBaseType(Variant input) {
    return Row.withSchema(BASE_SCHEMA).addValues(input.metadata(), input.value()).build();
  }

  @Override
  public Variant toInputType(Row base) {
    byte[] metadata = base.getBytes(0);
    byte[] value = base.getBytes(1);
    if (metadata == null || value == null) {
      throw new IllegalArgumentException(
          "Variant representation row must have non-null metadata and value, got " + base);
    }
    return Variant.of(metadata, value);
  }
}
