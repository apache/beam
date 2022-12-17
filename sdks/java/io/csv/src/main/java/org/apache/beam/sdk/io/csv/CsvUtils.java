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

import static org.apache.beam.sdk.io.csv.CsvPayloadSerializerProvider.CSV_FORMAT_PARAMETER_FIELD;
import static org.apache.beam.sdk.io.csv.CsvPayloadSerializerProvider.CSV_PAYLOAD_SERIALIZER_PARAMETER_SCHEMA;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializerProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.commons.csv.CSVFormat;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utilities to convert between CSV records, Beam rows, and user types. */
public final class CsvUtils {

  /**
   * Planned for: TODO(https://github.com/apache/beam/issues/24552) Returns a {@link SimpleFunction}
   * that converts a CSV String record to a Beam {@link Row}. Providing schemaFields determines the
   * subset and order of beamSchema fields expected from the CSV String record. Otherwise, the
   * expected order derives from the {@link Schema#sorted()} field order.
   */
  public static SimpleFunction<String, Row> getCsvStringToRowFunction(
      Schema beamSchema, @Nullable CSVFormat format, @Nullable List<String> schemaFields) {
    return null;
  }

  /**
   * Returns a {@link SerializableFunction} that converts a {@link Row} to a CSV String record.
   * Providing schemaFields determines the subset and order of beamSchema fields of the resulting
   * CSV String record. Otherwise, the order derives from the {@link Schema#sorted()} field order.
   */
  public static SerializableFunction<Row, String> getRowToCsvStringFunction(
      Schema beamSchema, @Nullable CSVFormat format, @Nullable List<String> schemaFields) {

    return null;
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
