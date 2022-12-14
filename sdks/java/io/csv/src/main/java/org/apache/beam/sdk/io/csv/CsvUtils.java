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
package org.apache.beam.sdk.io.csv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.commons.csv.CSVFormat;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utilities to convert between CSV records, Beam rows, and user types. */
public final class CsvUtils {

  /**
   * The valid {@link Schema.FieldType} from which {@link CsvIO} converts CSV records.
   *
   * <p>{@link FieldType#BYTE}
   *
   * <p>{@link FieldType#BOOLEAN}
   *
   * <p>{@link FieldType#DATETIME}
   *
   * <p>{@link FieldType#DECIMAL}
   *
   * <p>{@link FieldType#DOUBLE}
   *
   * <p>{@link FieldType#INT16}
   *
   * <p>{@link FieldType#INT32}
   *
   * <p>{@link FieldType#INT64}
   *
   * <p>{@link FieldType#FLOAT}
   *
   * <p>{@link FieldType#STRING}
   */
  public static final Set<FieldType> VALID_FIELD_TYPE_SET =
      ImmutableSet.of(
          FieldType.BYTE,
          FieldType.BOOLEAN,
          FieldType.DATETIME,
          FieldType.DECIMAL,
          FieldType.DOUBLE,
          FieldType.INT16,
          FieldType.INT32,
          FieldType.INT64,
          FieldType.FLOAT,
          FieldType.STRING);

  /**
   * Planned for: TODO(https://github.com/apache/beam/issues/24552) Returns a {@link SimpleFunction}
   * that converts a CSV byte[] record to a Beam {@link Row}. Providing schemaFields determines the
   * subset and order of beamSchema fields expected from the CSV byte[] record. Otherwise, the
   * expected order derives from the {@link Schema#sorted()} field order.
   */
  public static SimpleFunction<byte[], Row> getCsvBytesToRowFunction(
      Schema beamSchema, @Nullable CSVFormat format, @Nullable List<String> schemaFields) {
    CsvPayloadSerializer payloadSerializer =
        new CsvPayloadSerializer(beamSchema, format, schemaFields);
    return new CsvBytesToRowFn(payloadSerializer);
  }

  /**
   * Planned for: TODO(https://github.com/apache/beam/issues/24552) Returns a {@link SimpleFunction}
   * that converts a CSV String record to a Beam {@link Row}. Providing schemaFields determines the
   * subset and order of beamSchema fields expected from the CSV String record. Otherwise, the
   * expected order derives from the {@link Schema#sorted()} field order.
   */
  public static SimpleFunction<String, Row> getCsvStringToRowFunction(
      Schema beamSchema, @Nullable CSVFormat format, @Nullable List<String> schemaFields) {
    CsvPayloadSerializer payloadSerializer =
        new CsvPayloadSerializer(beamSchema, format, schemaFields);
    return new CsvStringToRowFn(payloadSerializer);
  }

  /**
   * Returns a {@link SimpleFunction} that converts a {@link Row} to a CSV byte[] record. Providing
   * schemaFields determines the subset and order of beamSchema fields of the resulting CSV byte[]
   * record. Otherwise, the order derives from the {@link Schema#sorted()} field order.
   */
  public static SimpleFunction<Row, byte[]> getRowToCsvBytesFunction(
      Schema beamSchema, @Nullable CSVFormat format, @Nullable List<String> schemaFields) {
    CsvPayloadSerializer payloadSerializer =
        new CsvPayloadSerializer(beamSchema, format, schemaFields);
    return new RowToCsvBytesFn(payloadSerializer);
  }

  /**
   * Returns a {@link SerializableFunction} that converts a {@link Row} to a CSV String record.
   * Providing schemaFields determines the subset and order of beamSchema fields of the resulting
   * CSV String record. Otherwise, the order derives from the {@link Schema#sorted()} field order.
   */
  public static SerializableFunction<Row, String> getRowToCsvStringFunction(
      Schema beamSchema, @Nullable CSVFormat format, @Nullable List<String> schemaFields) {

    CsvPayloadSerializer payloadSerializer =
        new CsvPayloadSerializer(beamSchema, format, schemaFields);
    return new RowToCsvStringFn(payloadSerializer);
  }

  /** Formats columns into a header String based on {@link CSVFormat}. */
  static String buildHeaderFrom(List<String> columns, CSVFormat csvFormat) {
    StringBuilder builder = new StringBuilder();
    try {
      boolean newRecord = true;
      for (String name : columns) {
        csvFormat.print(name, builder, newRecord);
        newRecord = false;
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return builder.toString();
  }

  /** Formats {@link Schema#sorted()} list of fields into a header based upon {@link CSVFormat}. */
  static String buildHeaderFrom(Schema schema, CSVFormat csvFormat) {
    return buildHeaderFrom(schema.sorted().getFieldNames(), csvFormat);
  }

  /** Checks columns against the schema. */
  static void validateHeaderAgainstSchema(@Nullable List<String> columns, Schema schema) {
    if (columns == null) {
      return;
    }
    if (columns.isEmpty()) {
      throw new IllegalArgumentException(
          "Columns is empty. An intent to not override columns, should assign null to the columns parameter.");
    }
    List<String> mismatch = new ArrayList<>();
    for (String name : columns) {
      if (!schema.hasField(name)) {
        mismatch.add(name);
      }
    }
    if (!mismatch.isEmpty()) {
      String mismatchString = String.join(", ", mismatch);
      String schemaString = String.join(", ", schema.getFieldNames());
      throw new IllegalArgumentException(
          String.format("schema: [%s] missing columns: [%s]", schemaString, mismatchString));
    }
  }

  /**
   * Validates whether the schema is flat i.e. only contains {@link #VALID_FIELD_TYPE_SET} {@link
   * FieldType}s.
   */
  static void validateSchema(Schema schema) {
    if (schema.getFieldCount() == 0) {
      throw new IllegalArgumentException("schema is empty");
    }
    List<String> invalidFields = new ArrayList<>();
    for (Field field : schema.getFields()) {
      if (!VALID_FIELD_TYPE_SET.contains(field.getType())) {
        invalidFields.add(field.toString());
      }
    }
    if (!invalidFields.isEmpty()) {
      String invalidFieldsMessage = String.join(", ", invalidFields);
      throw new IllegalArgumentException(
          String.format("CSV should only contain flat fields but found: %s", invalidFieldsMessage));
    }
  }

  /** A {@link CsvToRowFn} extension for CSV byte[] records. */
  static class CsvBytesToRowFn extends CsvToRowFn<byte[]> {
    CsvBytesToRowFn(CsvPayloadSerializer payloadSerializer) {
      super(payloadSerializer);
    }

    @Override
    byte[] toBytes(byte[] input) {
      return input;
    }
  }

  /** A {@link CsvToRowFn} extension for CSV String records. */
  static class CsvStringToRowFn extends CsvToRowFn<String> {
    CsvStringToRowFn(CsvPayloadSerializer payloadSerializer) {
      super(payloadSerializer);
    }

    @Override
    byte[] toBytes(String input) {
      return input.getBytes(StandardCharsets.UTF_8);
    }
  }

  /** A {@link RowToCsvFn} extension for CSV byte[] records. */
  static class RowToCsvBytesFn extends RowToCsvFn<byte[]> {
    RowToCsvBytesFn(CsvPayloadSerializer payloadSerializer) {
      super(payloadSerializer);
    }

    @Override
    byte[] fromBytes(byte[] input) {
      return input;
    }
  }

  /** A {@link RowToCsvFn} extension for CSV String records. */
  static class RowToCsvStringFn extends RowToCsvFn<String> {
    RowToCsvStringFn(CsvPayloadSerializer payloadSerializer) {
      super(payloadSerializer);
    }

    @Override
    String fromBytes(byte[] input) {
      return new String(input, StandardCharsets.UTF_8);
    }
  }

  /** Converts a CSV record to a Beam {@link Row}. */
  abstract static class CsvToRowFn<CsvT> extends SimpleFunction<CsvT, Row> {
    private final CsvPayloadSerializer payloadSerializer;

    CsvToRowFn(CsvPayloadSerializer payloadSerializer) {
      this.payloadSerializer = payloadSerializer;
    }

    abstract byte[] toBytes(CsvT input);

    @Override
    public Row apply(CsvT input) {
      byte[] bytes = toBytes(input);
      return payloadSerializer.deserialize(bytes);
    }
  }

  /** Converts a Beam {@link Row} to a CSV record. */
  abstract static class RowToCsvFn<CsvT> extends SimpleFunction<Row, CsvT> {
    private final CsvPayloadSerializer payloadSerializer;

    RowToCsvFn(CsvPayloadSerializer payloadSerializer) {
      this.payloadSerializer = payloadSerializer;
    }

    abstract CsvT fromBytes(byte[] input);

    @Override
    public CsvT apply(Row input) {
      byte[] bytes = payloadSerializer.serialize(input);
      return fromBytes(bytes);
    }
  }
}
