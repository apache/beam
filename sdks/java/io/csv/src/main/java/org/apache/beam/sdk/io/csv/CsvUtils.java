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

import java.nio.charset.StandardCharsets;
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

  /**
   * Returns a {@link SimpleFunction} that converts a CSV byte[] record to a {@param T} user type.
   */
  public static <T> SimpleFunction<byte[], T> getCsvBytesToUserTypeFunction(
      Class<T> userType, Schema beamSchema, @Nullable CSVFormat format) {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(beamSchema, format);
    return new CsvBytesToUserTypeFn<>(payloadSerializer, TypeDescriptor.of(userType));
  }

  /**
   * Returns a {@link SimpleFunction} that converts a CSV String record to a {@param T} user type.
   */
  public static <T> SimpleFunction<String, T> getCsvStringToUserTypeFunction(
      Class<T> userType, Schema beamSchema, @Nullable CSVFormat format) {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(beamSchema, format);
    return new CsvStringToUserTypeFn<>(payloadSerializer, TypeDescriptor.of(userType));
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

  /**
   * Returns a {@link SimpleFunction} that converts a {@param T} user type to a CSV byte[] record.
   */
  public static <T> SimpleFunction<T, byte[]> getUserTypeToBytesFunction(
      Class<T> userType, Schema beamSchema, @Nullable CSVFormat format) {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(beamSchema, format);
    return new UserTypeToCsvBytesFn<>(payloadSerializer, TypeDescriptor.of(userType));
  }

  /**
   * Returns a {@link SimpleFunction} that converts a {@param T} user type to a CSV String record.
   */
  public static <T> SimpleFunction<T, String> getUserTypeToStringFunction(
      Class<T> userType, Schema beamSchema, @Nullable CSVFormat format) {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(beamSchema, format);
    return new UserTypeToCsvStringFn<>(payloadSerializer, TypeDescriptor.of(userType));
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

  /** A {@link CsvToUserTypeFn} extension for CSV byte[] records. */
  static class CsvBytesToUserTypeFn<UserT> extends CsvToUserTypeFn<byte[], UserT> {

    CsvBytesToUserTypeFn(
        CsvPayloadSerializer payloadSerializer, TypeDescriptor<UserT> typeDescriptor) {
      super(payloadSerializer, typeDescriptor);
    }

    @Override
    byte[] toBytes(byte[] input) {
      return input;
    }
  }

  /** A {@link CsvToUserTypeFn} extension for CSV String records. */
  static class CsvStringToUserTypeFn<UserT> extends CsvToUserTypeFn<String, UserT> {

    CsvStringToUserTypeFn(
        CsvPayloadSerializer payloadSerializer, TypeDescriptor<UserT> typeDescriptor) {
      super(payloadSerializer, typeDescriptor);
    }

    @Override
    byte[] toBytes(String input) {
      return input.getBytes(StandardCharsets.UTF_8);
    }
  }

  /** A {@link UserTypeToCSVFn} extension for CSV byte[] records. */
  static class UserTypeToCsvBytesFn<UserT> extends UserTypeToCSVFn<UserT, byte[]> {
    UserTypeToCsvBytesFn(
        CsvPayloadSerializer payloadSerializer, TypeDescriptor<UserT> typeDescriptor) {
      super(payloadSerializer, typeDescriptor);
    }

    @Override
    byte[] fromBytes(byte[] input) {
      return input;
    }
  }

  /** A {@link UserTypeToCSVFn} extension for CSV String records. */
  static class UserTypeToCsvStringFn<UserT> extends UserTypeToCSVFn<UserT, String> {
    UserTypeToCsvStringFn(
        CsvPayloadSerializer payloadSerializer, TypeDescriptor<UserT> typeDescriptor) {
      super(payloadSerializer, typeDescriptor);
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

  /** Converts a CSV {@param CsvT} record to a {@param UserT} user type. */
  abstract static class CsvToUserTypeFn<CsvT, UserT> extends SimpleFunction<CsvT, UserT> {
    private static final DefaultSchemaProvider DEFAULT_SCHEMA_PROVIDER =
        new DefaultSchemaProvider();
    private final CsvPayloadSerializer payloadSerializer;
    private final TypeDescriptor<UserT> typeDescriptor;

    CsvToUserTypeFn(CsvPayloadSerializer payloadSerializer, TypeDescriptor<UserT> typeDescriptor) {
      this.payloadSerializer = payloadSerializer;
      this.typeDescriptor = typeDescriptor;
    }

    abstract byte[] toBytes(CsvT input);

    @Override
    public UserT apply(CsvT input) {
      byte[] bytes = toBytes(input);
      Row row = payloadSerializer.deserialize(bytes);
      return DEFAULT_SCHEMA_PROVIDER.fromRowFunction(typeDescriptor).apply(row);
    }
  }

  /** Converts a {@param UserT} user type to a {@param CsvT} CSV record. */
  abstract static class UserTypeToCSVFn<UserT, CsvT> extends SimpleFunction<UserT, CsvT> {
    private static final DefaultSchemaProvider DEFAULT_SCHEMA_PROVIDER =
        new DefaultSchemaProvider();
    private final CsvPayloadSerializer payloadSerializer;
    private final TypeDescriptor<UserT> typeDescriptor;

    UserTypeToCSVFn(CsvPayloadSerializer payloadSerializer, TypeDescriptor<UserT> typeDescriptor) {
      this.payloadSerializer = payloadSerializer;
      this.typeDescriptor = typeDescriptor;
    }

    abstract CsvT fromBytes(byte[] input);

    @Override
    public CsvT apply(UserT input) {
      Row row = DEFAULT_SCHEMA_PROVIDER.toRowFunction(typeDescriptor).apply(input);
      byte[] bytes = payloadSerializer.serialize(row);
      return fromBytes(bytes);
    }
  }
}
