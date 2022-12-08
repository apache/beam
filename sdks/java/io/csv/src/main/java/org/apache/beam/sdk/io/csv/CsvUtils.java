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
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema.DefaultSchemaProvider;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.csv.CSVFormat;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utilities to convert between CSV records, Beam rows, and user types. */
public final class CsvUtils {

  /** Returns a {@link SimpleFunction} that converts a CSV byte[] record to a Beam {@link Row}. */
  public static SimpleFunction<byte[], Row> getCsvBytesToRowFunction(
      Schema beamSchema, @Nullable CSVFormat format) {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(beamSchema, format);
    return new CsvBytesToRowFn(payloadSerializer);
  }

  /** Returns a {@link SimpleFunction} that converts a CSV String record to a Beam {@link Row}. */
  public static SimpleFunction<String, Row> getCsvStringToRowFunction(
      Schema beamSchema, @Nullable CSVFormat format) {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(beamSchema, format);
    return new CsvStringToRowFn(payloadSerializer);
  }

  /** Returns a {@link SimpleFunction} that converts a {@link Row} to a CSV byte[] record. */
  public static SimpleFunction<Row, byte[]> getRowToCsvBytesFunction(
      Schema beamSchema, @Nullable CSVFormat format) {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(beamSchema, format);
    return new RowToCsvBytesFn(payloadSerializer);
  }

  /** Returns a {@link SimpleFunction} that converts a {@link Row} to a CSV String record. */
  public static SimpleFunction<Row, String> getRowToCsvStringFunction(
      Schema beamSchema, @Nullable CSVFormat format) {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(beamSchema, format);
    return new RowToCsvStringFn(payloadSerializer);
  }

  static String buildHeaderFrom(Schema schema, CSVFormat csvFormat) {
    StringBuilder builder = new StringBuilder();
    try {
      boolean newRecord = true;
      for (int i = 0; i < schema.getFieldCount(); i++) {
        String name = schema.getField(i).getName();
        csvFormat.print(name, builder, newRecord);
        newRecord = false;
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return builder.toString();
  }

  static void validateSchemaAndHeader(String header, Schema schema, CSVFormat csvFormat) {
    StringReader reader = new StringReader(header);
    try {
      List<String> headerNames = csvFormat.parse(reader).getHeaderNames();
      if (headerNames.isEmpty()) {
        throw new IllegalArgumentException(String.format("failed to parse header names from %s", header));
      }
      for (String name : headerNames) {
        if (!schema.hasField(name)) {
          throw new IllegalArgumentException(String.format("column: %s does not match name in schema: %s", name, schema));
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
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

  /** Converts a CSV {@param CsvT} record to a Beam {@link Row}. */
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

  /** Converts a Beam {@link Row} to a CSV {@param CsvT} record. */
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
