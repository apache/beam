package org.apache.beam.sdk.io.csv;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema.DefaultSchemaProvider;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.csv.CSVFormat;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CsvUtils {
  public static SimpleFunction<byte[], Row> getCsvBytesToRowFunction(Schema beamSchema, @Nullable CSVFormat format) {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(beamSchema, format);
    return new CsvBytesToRowFn(payloadSerializer);
  }

  public static SimpleFunction<String, Row> getCsvStringToRowFunction(Schema beamSchema, @Nullable CSVFormat format) {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(beamSchema, format);
    return new CsvStringToRowFn(payloadSerializer);
  }

  public static SimpleFunction<Row, byte[]> getRowToCsvBytesFunction(Schema beamSchema, @Nullable CSVFormat format) {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(beamSchema, format);
    return new RowToCsvBytesFn(payloadSerializer);
  }

  public static SimpleFunction<Row, String> getRowToCsvStringFunction(Schema beamSchema, @Nullable CSVFormat format) {
    CsvPayloadSerializer payloadSerializer = new CsvPayloadSerializer(beamSchema, format);
    return new RowToCsvStringFn(payloadSerializer);
  }

  static class CsvBytesToRowFn extends CsvToRowFn<byte[]> {
    CsvBytesToRowFn(CsvPayloadSerializer payloadSerializer) {
      super(payloadSerializer);
    }

    @Override
    byte[] toBytes(byte[] input) {
      return input;
    }
  }

  static class CsvStringToRowFn extends CsvToRowFn<String> {
    CsvStringToRowFn(CsvPayloadSerializer payloadSerializer) {
      super(payloadSerializer);
    }

    @Override
    byte[] toBytes(String input) {
      return input.getBytes(StandardCharsets.UTF_8);
    }
  }

  static class RowToCsvBytesFn extends RowToCsvFn<byte[]> {
    RowToCsvBytesFn(CsvPayloadSerializer payloadSerializer) {
      super(payloadSerializer);
    }

    @Override
    byte[] fromBytes(byte[] input) {
      return input;
    }
  }

  static class RowToCsvStringFn extends RowToCsvFn<String> {
    RowToCsvStringFn(CsvPayloadSerializer payloadSerializer) {
      super(payloadSerializer);
    }

    @Override
    String fromBytes(byte[] input) {
      return new String(input, StandardCharsets.UTF_8);
    }
  }

  static abstract class CsvToRowFn<CsvT> extends SimpleFunction<CsvT, Row> {
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

  static abstract class RowToCsvFn<CsvT> extends SimpleFunction<Row, CsvT> {
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

  static abstract class CsvToUserTypeFn<CsvT, UserT> extends SimpleFunction<CsvT, UserT> {
    private static final DefaultSchemaProvider DEFAULT_SCHEMA_PROVIDER = new DefaultSchemaProvider();
    private final CsvPayloadSerializer payloadSerializer;
    private final TypeDescriptor<UserT> typeDescriptor;

    CsvToUserTypeFn(CsvPayloadSerializer payloadSerializer,
        TypeDescriptor<UserT> typeDescriptor) {
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

  static abstract class UserTypeToCSVFn<UserT, CsvT> extends SimpleFunction<UserT, CsvT> {
    private static final DefaultSchemaProvider DEFAULT_SCHEMA_PROVIDER = new DefaultSchemaProvider();
    private final CsvPayloadSerializer payloadSerializer;
    private final TypeDescriptor<UserT> typeDescriptor;

    UserTypeToCSVFn(CsvPayloadSerializer payloadSerializer,
        TypeDescriptor<UserT> typeDescriptor) {
      this.payloadSerializer = payloadSerializer;
      this.typeDescriptor = typeDescriptor;
    }

    abstract CsvT fromBytes(byte[] input);

    @Override
    public CsvT apply(UserT input) {
      Row row =
    }
  }
}
