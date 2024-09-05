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

import static org.apache.beam.sdk.schemas.SchemaTranslation.STANDARD_LOGICAL_TYPES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.model.pipeline.v1.SchemaApi.ArrayType;
import org.apache.beam.model.pipeline.v1.SchemaApi.ArrayTypeValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.AtomicType;
import org.apache.beam.model.pipeline.v1.SchemaApi.AtomicTypeValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.FieldValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.LogicalType;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.DateTime;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.schemas.logicaltypes.FixedPrecisionNumeric;
import org.apache.beam.sdk.schemas.logicaltypes.FixedString;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.PythonCallable;
import org.apache.beam.sdk.schemas.logicaltypes.SchemaLogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.logicaltypes.UnknownLogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.VariableBytes;
import org.apache.beam.sdk.schemas.logicaltypes.VariableString;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link SchemaTranslation}. */
@RunWith(Enclosed.class)
public class SchemaTranslationTest {

  /** Tests round-trip proto encodings for {@link Schema}. */
  @RunWith(Parameterized.class)
  public static class ToFromProtoTest {
    @Parameters(name = "{index}: {0}")
    public static Iterable<Schema> data() {
      Map<String, Integer> optionMap = new HashMap<>();
      optionMap.put("string", 42);
      List<String> optionList = new ArrayList<>();
      optionList.add("string");
      Row optionRow =
          Row.withSchema(
                  Schema.builder()
                      .addField("field_one", FieldType.STRING)
                      .addField("field_two", FieldType.INT32)
                      .build())
              .addValue("value")
              .addValue(42)
              .build();

      Schema.Options.Builder optionsBuilder =
          Schema.Options.builder()
              .setOption("field_option_boolean", FieldType.BOOLEAN, true)
              .setOption("field_option_byte", FieldType.BYTE, (byte) 12)
              .setOption("field_option_int16", FieldType.INT16, (short) 12)
              .setOption("field_option_int32", FieldType.INT32, 12)
              .setOption("field_option_int64", FieldType.INT64, 12L)
              .setOption("field_option_string", FieldType.STRING, "foo")
              .setOption("field_option_bytes", FieldType.BYTES, new byte[] {0x42, 0x69, 0x00})
              .setOption("field_option_float", FieldType.FLOAT, (float) 12.0)
              .setOption("field_option_double", FieldType.DOUBLE, 12.0)
              .setOption(
                  "field_option_map", FieldType.map(FieldType.STRING, FieldType.INT32), optionMap)
              .setOption("field_option_array", FieldType.array(FieldType.STRING), optionList)
              .setOption("field_option_row", optionRow)
              .setOption("field_option_value", FieldType.STRING, "other");

      return ImmutableList.<Schema>builder()
          .add(Schema.of(Field.of("string", FieldType.STRING)))
          .add(
              Schema.of(
                  Field.of("boolean", FieldType.BOOLEAN),
                  Field.of("byte", FieldType.BYTE),
                  Field.of("int16", FieldType.INT16),
                  Field.of("int32", FieldType.INT32),
                  Field.of("int64", FieldType.INT64)))
          .add(
              Schema.of(
                  Field.of(
                      "row",
                      FieldType.row(
                          Schema.of(
                              Field.of("foo", FieldType.STRING),
                              Field.of("bar", FieldType.DOUBLE),
                              Field.of("baz", FieldType.BOOLEAN))))))
          .add(
              Schema.of(
                  Field.of(
                      "array(array(int64)))",
                      FieldType.array(FieldType.array(FieldType.INT64.withNullable(true))))))
          .add(
              Schema.of(
                  Field.of(
                      "iter(iter(int64)))",
                      FieldType.iterable(FieldType.iterable(FieldType.INT64.withNullable(true))))))
          .add(
              Schema.of(
                  Field.of("nullable", FieldType.STRING.withNullable(true)),
                  Field.of("non_nullable", FieldType.STRING.withNullable(false))))
          .add(
              Schema.of(
                  Field.of("decimal", FieldType.DECIMAL), Field.of("datetime", FieldType.DATETIME)))
          .add(Schema.of(Field.of("fixed_bytes", FieldType.logicalType(FixedBytes.of(24)))))
          .add(Schema.of(Field.of("micros_instant", FieldType.logicalType(new MicrosInstant()))))
          .add(Schema.of(Field.of("python_callable", FieldType.logicalType(new PythonCallable()))))
          .add(
              Schema.of(
                      Field.of("field_with_option_atomic", FieldType.STRING)
                          .withOptions(
                              Schema.Options.builder()
                                  .setOption(
                                      "field_option_atomic", FieldType.INT32, Integer.valueOf(42))
                                  .build()))
                  .withOptions(
                      Schema.Options.builder()
                          .setOption("schema_option_atomic", FieldType.BOOLEAN, true)))
          .add(
              Schema.of(
                      Field.of("field_with_option_map", FieldType.STRING)
                          .withOptions(
                              Schema.Options.builder()
                                  .setOption(
                                      "field_option_map",
                                      FieldType.map(FieldType.STRING, FieldType.INT32),
                                      optionMap)))
                  .withOptions(
                      Schema.Options.builder()
                          .setOption(
                              "field_option_map",
                              FieldType.map(FieldType.STRING, FieldType.INT32),
                              optionMap)))
          .add(
              Schema.of(
                      Field.of("field_with_option_array", FieldType.STRING)
                          .withOptions(
                              Schema.Options.builder()
                                  .setOption(
                                      "field_option_array",
                                      FieldType.array(FieldType.STRING),
                                      optionList)
                                  .build()))
                  .withOptions(
                      Schema.Options.builder()
                          .setOption(
                              "field_option_array", FieldType.array(FieldType.STRING), optionList)))
          .add(
              Schema.of(Field.of("field", FieldType.STRING).withOptions(optionsBuilder))
                  .withOptions(optionsBuilder))
          .add(
              Schema.of(
                  Field.of(
                      "null_argument", FieldType.logicalType(new PortableNullArgLogicalType()))))
          .add(Schema.of(Field.of("logical_argument", FieldType.logicalType(new DateTime()))))
          .add(
              Schema.of(Field.of("single_arg_argument", FieldType.logicalType(FixedBytes.of(100)))))
          .add(Schema.of(Field.of("schema", FieldType.logicalType(new SchemaLogicalType()))))
          .build();
    }

    @Parameter(0)
    public Schema schema;

    @Test
    public void toAndFromProto() throws Exception {
      SchemaApi.Schema schemaProto = SchemaTranslation.schemaToProto(schema, true);

      Schema decodedSchema = SchemaTranslation.schemaFromProto(schemaProto);
      assertThat(decodedSchema, equalTo(schema));
    }
  }

  /** Tests round-trip proto encodings for {@link Schema}. */
  @RunWith(Parameterized.class)
  public static class FromProtoToProtoTest {
    @Parameters(name = "{index}: {0}")
    public static Iterable<SchemaApi.Schema> data() {
      SchemaApi.Schema.Builder builder = SchemaApi.Schema.newBuilder();
      // A go 'int'
      builder.addFields(
          SchemaApi.Field.newBuilder()
              .setName("goInt")
              .setDescription("An int from go")
              .setType(
                  SchemaApi.FieldType.newBuilder()
                      .setLogicalType(
                          SchemaApi.LogicalType.newBuilder()
                              .setUrn("gosdk:int")
                              .setRepresentation(
                                  SchemaApi.FieldType.newBuilder()
                                      .setAtomicType(SchemaApi.AtomicType.INT64))
                              .build()))
              .setId(0)
              .setEncodingPosition(0)
              .build());
      // A pickled python object
      builder.addFields(
          SchemaApi.Field.newBuilder()
              .setName("pythonObject")
              .setType(
                  SchemaApi.FieldType.newBuilder()
                      .setLogicalType(
                          SchemaApi.LogicalType.newBuilder()
                              .setUrn("pythonsdk:value")
                              .setPayload(
                                  ByteString.copyFrom(
                                      "some payload describing a python type",
                                      StandardCharsets.UTF_8))
                              .setRepresentation(
                                  SchemaApi.FieldType.newBuilder()
                                      .setAtomicType(SchemaApi.AtomicType.BYTES))
                              .build()))
              .setId(1)
              .setEncodingPosition(1)
              .build());
      // An enum logical type that Java doesn't know about
      builder.addFields(
          SchemaApi.Field.newBuilder()
              .setName("enum")
              .setType(
                  SchemaApi.FieldType.newBuilder()
                      .setLogicalType(
                          LogicalType.newBuilder()
                              .setUrn("strange_enum")
                              .setArgumentType(
                                  SchemaApi.FieldType.newBuilder()
                                      .setArrayType(
                                          ArrayType.newBuilder()
                                              .setElementType(
                                                  SchemaApi.FieldType.newBuilder()
                                                      .setAtomicType(AtomicType.STRING))))
                              .setArgument(
                                  FieldValue.newBuilder()
                                      .setArrayValue(
                                          ArrayTypeValue.newBuilder()
                                              .addElement(
                                                  FieldValue.newBuilder()
                                                      .setAtomicValue(
                                                          AtomicTypeValue.newBuilder()
                                                              .setString("FOO")
                                                              .build())
                                                      .build())
                                              .addElement(
                                                  FieldValue.newBuilder()
                                                      .setAtomicValue(
                                                          AtomicTypeValue.newBuilder()
                                                              .setString("BAR")
                                                              .build())
                                                      .build())
                                              .build())
                                      .build())
                              .setRepresentation(
                                  SchemaApi.FieldType.newBuilder().setAtomicType(AtomicType.BYTE))
                              .build()))
              .setId(2)
              .setEncodingPosition(2)
              .build());
      SchemaApi.Schema unknownLogicalTypeSchema = builder.build();

      return ImmutableList.<SchemaApi.Schema>builder().add(unknownLogicalTypeSchema).build();
    }

    @Parameter(0)
    public SchemaApi.Schema schemaProto;

    @Test
    public void fromProtoAndToProto() throws Exception {
      Schema decodedSchema = SchemaTranslation.schemaFromProto(schemaProto);

      SchemaApi.Schema reencodedSchemaProto = SchemaTranslation.schemaToProto(decodedSchema, true);
      reencodedSchemaProto = reencodedSchemaProto.toBuilder().clearId().build();

      assertThat(reencodedSchemaProto, equalTo(schemaProto));
    }
  }

  /** Tests round-trip proto encodings for {@link Row}. */
  @RunWith(Parameterized.class)
  public static class RowToFromProtoTest {

    public static Row simpleRow(FieldType type, Object value) {
      return Row.withSchema(Schema.of(Field.of("s", type))).addValue(value).build();
    }

    public static Row simpleNullRow(FieldType type) {
      return Row.withSchema(Schema.of(Field.nullable("s", type))).addValue(null).build();
    }

    @Parameters(name = "{index}: {0}")
    public static Iterable<Row> data() {
      Map<String, Integer> map = new HashMap<>();
      map.put("string", 42);
      List<String> list = new ArrayList<>();
      list.add("string");
      Schema schema =
          Schema.builder()
              .addField("field_one", FieldType.STRING)
              .addField("field_two", FieldType.INT32)
              .build();
      Row row = Row.withSchema(schema).addValue("value").addValue(42).build();

      return ImmutableList.<Row>builder()
          .add(simpleRow(FieldType.STRING, "string"))
          .add(simpleRow(FieldType.BOOLEAN, true))
          .add(simpleRow(FieldType.BYTE, (byte) 12))
          .add(simpleRow(FieldType.INT16, (short) 12))
          .add(simpleRow(FieldType.INT32, 12))
          .add(simpleRow(FieldType.INT64, 12L))
          .add(simpleRow(FieldType.BYTES, new byte[] {0x42, 0x69, 0x00}))
          .add(simpleRow(FieldType.FLOAT, (float) 12))
          .add(simpleRow(FieldType.DOUBLE, 12.0))
          .add(simpleRow(FieldType.map(FieldType.STRING, FieldType.INT32), map))
          .add(simpleRow(FieldType.array(FieldType.STRING), list))
          .add(simpleRow(FieldType.row(row.getSchema()), row))
          .add(simpleRow(FieldType.DATETIME, new Instant(23L)))
          .add(simpleRow(FieldType.DECIMAL, BigDecimal.valueOf(100000)))
          .add(simpleRow(FieldType.logicalType(new PortableNullArgLogicalType()), "str"))
          .add(simpleRow(FieldType.logicalType(new DateTime()), LocalDateTime.of(2000, 1, 3, 3, 1)))
          .add(simpleNullRow(FieldType.STRING))
          .add(simpleNullRow(FieldType.INT32))
          .add(simpleNullRow(FieldType.map(FieldType.STRING, FieldType.INT32)))
          .add(simpleNullRow(FieldType.array(FieldType.STRING)))
          .add(simpleNullRow(FieldType.row(row.getSchema())))
          .add(simpleNullRow(FieldType.logicalType(new PortableNullArgLogicalType())))
          .add(simpleNullRow(FieldType.logicalType(new DateTime())))
          .add(simpleNullRow(FieldType.DECIMAL))
          .add(simpleNullRow(FieldType.DATETIME))
          .build();
    }

    @Parameter(0)
    public Row row;

    @Test
    public void toAndFromProto() throws Exception {
      SchemaApi.Row rowProto = SchemaTranslation.rowToProto(row);
      Row decodedRow =
          (Row) SchemaTranslation.rowFromProto(rowProto, FieldType.row(row.getSchema()));
      assertThat(decodedRow, equalTo(row));
    }
  }

  /** Tests that we raise helpful errors when decoding bad {@link Schema} protos. */
  @RunWith(JUnit4.class)
  public static class DecodeErrorTest {

    @Test
    public void typeInfoNotSet() {
      SchemaApi.Schema.Builder builder = SchemaApi.Schema.newBuilder();

      builder.addFields(
          SchemaApi.Field.newBuilder()
              .setName("field_no_typeInfo")
              .setType(SchemaApi.FieldType.newBuilder())
              .setId(0)
              .setEncodingPosition(0)
              .build());

      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> {
                SchemaTranslation.schemaFromProto(builder.build());
              });

      assertThat(exception.getMessage(), containsString("field_no_typeInfo"));
      assertThat(exception.getCause().getMessage(), containsString("TYPEINFO_NOT_SET"));
    }
  }

  /** Test schema translation of logical types. */
  @RunWith(Parameterized.class)
  public static class LogicalTypesTest {
    @Parameters(name = "{index}: {0}")
    public static Iterable<Schema.FieldType> data() {
      return ImmutableList.<Schema.FieldType>builder()
          .add(FieldType.logicalType(SqlTypes.DATE))
          .add(FieldType.logicalType(SqlTypes.TIME))
          .add(FieldType.logicalType(SqlTypes.DATETIME))
          .add(FieldType.logicalType(SqlTypes.TIMESTAMP))
          .add(FieldType.logicalType(new NanosInstant()))
          .add(FieldType.logicalType(new NanosDuration()))
          .add(FieldType.logicalType(FixedBytes.of(10)))
          .add(FieldType.logicalType(VariableBytes.of(10)))
          .add(FieldType.logicalType(FixedString.of(10)))
          .add(FieldType.logicalType(VariableString.of(10)))
          .add(FieldType.logicalType(FixedPrecisionNumeric.of(10)))
          .add(FieldType.logicalType(new PortableNullArgLogicalType()))
          .add(FieldType.logicalType(new NullArgumentLogicalType()))
          .build();
    }

    @Parameter(0)
    public Schema.FieldType fieldType;

    @Test
    public void testLogicalTypeSerializeDeserilizeCorrectly() {
      SchemaApi.FieldType proto = SchemaTranslation.fieldTypeToProto(fieldType, true);
      Schema.FieldType translated = SchemaTranslation.fieldTypeFromProto(proto);

      assertThat(
          translated.getLogicalType().getClass(), equalTo(fieldType.getLogicalType().getClass()));
      assertThat(
          translated.getLogicalType().getArgumentType(),
          equalTo(fieldType.getLogicalType().getArgumentType()));
      assertThat(
          translated.getLogicalType().getArgument(),
          equalTo(fieldType.getLogicalType().getArgument()));
      assertThat(
          translated.getLogicalType().getIdentifier(),
          equalTo(fieldType.getLogicalType().getIdentifier()));
    }

    @Test
    public void testLogicalTypeFromToProtoCorrectly() {
      SchemaApi.FieldType proto = SchemaTranslation.fieldTypeToProto(fieldType, false);
      Schema.FieldType translated = SchemaTranslation.fieldTypeFromProto(proto);

      if (STANDARD_LOGICAL_TYPES.containsKey(translated.getLogicalType().getIdentifier())) {
        // standard logical type should be able to fully recover the original type
        assertThat(
            translated.getLogicalType().getClass(), equalTo(fieldType.getLogicalType().getClass()));
      } else {
        // non-standard type will get assembled to UnknownLogicalType
        assertThat(translated.getLogicalType().getClass(), equalTo(UnknownLogicalType.class));
      }
      assertThat(
          translated.getLogicalType().getArgumentType(),
          equalTo(fieldType.getLogicalType().getArgumentType()));
      assertThat(
          translated.getLogicalType().getArgument(),
          equalTo(fieldType.getLogicalType().getArgument()));
      if (fieldType.getLogicalType().getIdentifier().startsWith("beam:logical_type:")) {
        // portable logical type should fully recover the urn
        assertThat(
            translated.getLogicalType().getIdentifier(),
            equalTo(fieldType.getLogicalType().getIdentifier()));
      } else {
        // non-portable logical type would have "javasdk_<IDENTIFIER>" urn
        assertThat(
            translated.getLogicalType().getIdentifier(),
            equalTo(
                String.format(
                    "beam:logical_type:javasdk_%s:v1",
                    fieldType
                        .getLogicalType()
                        .getIdentifier()
                        .toLowerCase()
                        .replaceAll("[^0-9A-Za-z_]", ""))));
      }
    }
  }

  /** A portable logical type that has no argument. */
  private static class PortableNullArgLogicalType extends NullArgumentLogicalType {
    public static final String IDENTIFIER = "beam:logical_type:null_argument:v1";

    @Override
    public String getIdentifier() {
      return IDENTIFIER;
    }
  }

  /** A non-portable (Java SDK) logical type that has no argument. */
  private static class NullArgumentLogicalType implements Schema.LogicalType<String, String> {
    public static final String IDENTIFIER = "NULL_ARGUMENT";

    @Override
    public String toBaseType(String input) {
      return input;
    }

    @Override
    public String toInputType(String base) {
      return base;
    }

    @Override
    public String getIdentifier() {
      return IDENTIFIER;
    }

    @Override
    public @Nullable Schema.FieldType getArgumentType() {
      return null;
    }

    @Override
    public Schema.FieldType getBaseType() {
      return FieldType.STRING;
    }
  }
}
