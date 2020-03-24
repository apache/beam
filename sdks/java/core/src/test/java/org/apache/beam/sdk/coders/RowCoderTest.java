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
package org.apache.beam.sdk.coders;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType.Value;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

/** Unit tests for {@link RowCoder}. */
public class RowCoderTest {

  @Test
  public void testPrimitiveTypes() throws Exception {
    Schema schema =
        Schema.builder()
            .addByteField("f_byte")
            .addInt16Field("f_int16")
            .addInt32Field("f_int32")
            .addInt64Field("f_int64")
            .addDecimalField("f_decimal")
            .addFloatField("f_float")
            .addDoubleField("f_double")
            .addStringField("f_string")
            .addDateTimeField("f_datetime")
            .addBooleanField("f_boolean")
            .build();

    DateTime dateTime =
        new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4).withZone(DateTimeZone.UTC);
    Row row =
        Row.withSchema(schema)
            .addValues(
                (byte) 0, (short) 1, 2, 3L, new BigDecimal(2.3), 1.2f, 3.0d, "str", dateTime, false)
            .build();

    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  @Test
  public void testNestedTypes() throws Exception {
    Schema nestedSchema = Schema.builder().addInt32Field("f1_int").addStringField("f1_str").build();
    Schema schema =
        Schema.builder().addInt32Field("f_int").addRowField("nested", nestedSchema).build();

    Row nestedRow = Row.withSchema(nestedSchema).addValues(18, "foobar").build();
    Row row = Row.withSchema(schema).addValues(42, nestedRow).build();

    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  @Test
  public void testArrays() throws Exception {
    Schema schema = Schema.builder().addArrayField("f_array", FieldType.STRING).build();
    Row row = Row.withSchema(schema).addArray("one", "two", "three", "four").build();

    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  @Test
  public void testIterables() throws Exception {
    Schema schema = Schema.builder().addIterableField("f_iter", FieldType.STRING).build();
    Row row =
        Row.withSchema(schema).addIterable(ImmutableList.of("one", "two", "three", "four")).build();

    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  @Test
  public void testArrayOfRow() throws Exception {
    Schema nestedSchema = Schema.builder().addInt32Field("f1_int").addStringField("f1_str").build();
    FieldType collectionElementType = FieldType.row(nestedSchema);
    Schema schema = Schema.builder().addArrayField("f_array", collectionElementType).build();
    Row row =
        Row.withSchema(schema)
            .addArray(
                Row.withSchema(nestedSchema).addValues(1, "one").build(),
                Row.withSchema(nestedSchema).addValues(2, "two").build(),
                Row.withSchema(nestedSchema).addValues(3, "three").build())
            .build();

    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  @Test
  public void testIterableOfRow() throws Exception {
    Schema nestedSchema = Schema.builder().addInt32Field("f1_int").addStringField("f1_str").build();
    FieldType collectionElementType = FieldType.row(nestedSchema);
    Schema schema = Schema.builder().addIterableField("f_iter", collectionElementType).build();
    Row row =
        Row.withSchema(schema)
            .addIterable(
                ImmutableList.of(
                    Row.withSchema(nestedSchema).addValues(1, "one").build(),
                    Row.withSchema(nestedSchema).addValues(2, "two").build(),
                    Row.withSchema(nestedSchema).addValues(3, "three").build()))
            .build();

    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  @Test
  public void testArrayOfArray() throws Exception {
    FieldType arrayType = FieldType.array(FieldType.array(FieldType.INT32));
    Schema schema = Schema.builder().addField("f_array", arrayType).build();
    Row row =
        Row.withSchema(schema)
            .addArray(
                Lists.newArrayList(1, 2, 3, 4),
                Lists.newArrayList(5, 6, 7, 8),
                Lists.newArrayList(9, 10, 11, 12))
            .build();

    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  @Test
  public void testIterableOfIterable() throws Exception {
    FieldType iterableType = FieldType.iterable(FieldType.array(FieldType.INT32));
    Schema schema = Schema.builder().addField("f_iter", iterableType).build();
    Row row =
        Row.withSchema(schema)
            .addIterable(
                ImmutableList.of(
                    Lists.newArrayList(1, 2, 3, 4),
                    Lists.newArrayList(5, 6, 7, 8),
                    Lists.newArrayList(9, 10, 11, 12)))
            .build();

    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  @Test
  public void testLogicalType() throws Exception {
    EnumerationType enumeration = EnumerationType.create("one", "two", "three");
    Schema schema = Schema.builder().addLogicalTypeField("f_enum", enumeration).build();
    Row row = Row.withSchema(schema).addValue(enumeration.valueOf("two")).build();

    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  @Test
  public void testLogicalTypeInCollection() throws Exception {
    EnumerationType enumeration = EnumerationType.create("one", "two", "three");
    Schema schema =
        Schema.builder().addArrayField("f_enum_array", FieldType.logicalType(enumeration)).build();
    Row row =
        Row.withSchema(schema)
            .addArray(enumeration.valueOf("two"), enumeration.valueOf("three"))
            .build();

    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  private static class NestedLogicalType implements LogicalType<String, EnumerationType.Value> {
    EnumerationType enumeration;

    NestedLogicalType(EnumerationType enumeration) {
      this.enumeration = enumeration;
    }

    @Override
    public String getIdentifier() {
      return "";
    }

    @Override
    public FieldType getArgumentType() {
      return FieldType.STRING;
    }

    @Override
    public FieldType getBaseType() {
      return FieldType.logicalType(enumeration);
    }

    @Override
    public Value toBaseType(String input) {
      return enumeration.valueOf(input);
    }

    @Override
    public String toInputType(Value base) {
      return enumeration.toString(base);
    }
  }

  @Test
  public void testNestedLogicalTypes() throws Exception {
    EnumerationType enumeration = EnumerationType.create("one", "two", "three");
    Schema schema =
        Schema.builder()
            .addLogicalTypeField("f_nested_logical_type", new NestedLogicalType(enumeration))
            .build();
    Row row = Row.withSchema(schema).addValue("two").build();

    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  @Test(expected = NonDeterministicException.class)
  public void testVerifyDeterministic() throws NonDeterministicException {
    Schema schema =
        Schema.builder()
            .addField("f1", FieldType.DOUBLE)
            .addField("f2", FieldType.FLOAT)
            .addField("f3", FieldType.INT32)
            .build();
    RowCoder coder = RowCoder.of(schema);

    coder.verifyDeterministic();
  }

  @Test(expected = NonDeterministicException.class)
  public void testVerifyDeterministicNestedRow() throws NonDeterministicException {
    Schema schema =
        Schema.builder()
            .addField(
                "f1",
                FieldType.row(
                    Schema.builder()
                        .addField("a1", FieldType.DOUBLE)
                        .addField("a2", FieldType.INT64)
                        .build()))
            .build();
    RowCoder coder = RowCoder.of(schema);

    coder.verifyDeterministic();
  }

  @Test
  public void testConsistentWithEqualsBytesField() throws Exception {
    Schema schema = Schema.of(Schema.Field.of("f1", FieldType.BYTES));
    Row row1 = Row.withSchema(schema).addValue(new byte[] {1, 2, 3, 4}).build();
    Row row2 = Row.withSchema(schema).addValue(new byte[] {1, 2, 3, 4}).build();
    RowCoder coder = RowCoder.of(schema);

    Assume.assumeTrue(coder.consistentWithEquals());

    CoderProperties.coderConsistentWithEquals(coder, row1, row2);
  }

  @Test
  @Ignore
  public void testConsistentWithEqualsMapWithBytesKeyField() throws Exception {
    FieldType fieldType = FieldType.map(FieldType.BYTES, FieldType.INT32);
    Schema schema = Schema.of(Schema.Field.of("f1", fieldType));
    RowCoder coder = RowCoder.of(schema);

    Map<byte[], Integer> map1 = Collections.singletonMap(new byte[] {1, 2, 3, 4}, 1);
    Row row1 = Row.withSchema(schema).addValue(map1).build();

    Map<byte[], Integer> map2 = Collections.singletonMap(new byte[] {1, 2, 3, 4}, 1);
    Row row2 = Row.withSchema(schema).addValue(map2).build();

    Assume.assumeTrue(coder.consistentWithEquals());

    CoderProperties.coderConsistentWithEquals(coder, row1, row2);
  }

  @Test
  public void testConsistentWithEqualsArrayOfBytes() throws Exception {
    FieldType fieldType = FieldType.array(FieldType.BYTES);
    Schema schema = Schema.of(Schema.Field.of("f1", fieldType));
    RowCoder coder = RowCoder.of(schema);

    List<byte[]> list1 = Collections.singletonList(new byte[] {1, 2, 3, 4});
    Row row1 = Row.withSchema(schema).addValue(list1).build();

    List<byte[]> list2 = Collections.singletonList(new byte[] {1, 2, 3, 4});
    Row row2 = Row.withSchema(schema).addValue(list2).build();

    Assume.assumeTrue(coder.consistentWithEquals());

    CoderProperties.coderConsistentWithEquals(coder, row1, row2);
  }

  @Test
  public void testConsistentWithEqualsArrayOfArrayOfBytes() throws Exception {
    FieldType fieldType = FieldType.array(FieldType.array(FieldType.BYTES));
    Schema schema = Schema.of(Schema.Field.of("f1", fieldType));
    RowCoder coder = RowCoder.of(schema);

    List<byte[]> innerList1 = Collections.singletonList(new byte[] {1, 2, 3, 4});
    List<List<byte[]>> list1 = Collections.singletonList(innerList1);
    Row row1 = Row.withSchema(schema).addValue(list1).build();

    List<byte[]> innerList2 = Collections.singletonList(new byte[] {1, 2, 3, 4});
    List<List<byte[]>> list2 = Collections.singletonList(innerList2);
    Row row2 = Row.withSchema(schema).addValue(list2).build();

    Assume.assumeTrue(coder.consistentWithEquals());

    CoderProperties.coderConsistentWithEquals(coder, row1, row2);
  }

  @Test
  public void testConsistentWithEqualsArrayWithNull() throws Exception {
    Schema schema =
        Schema.builder()
            .addField("a", Schema.FieldType.array(Schema.FieldType.INT32.withNullable(true)))
            .build();

    Row row = Row.withSchema(schema).addValue(Arrays.asList(1, null)).build();
    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  @Test
  public void testConsistentWithEqualsIterableWithNull() throws Exception {
    Schema schema =
        Schema.builder()
            .addField("a", Schema.FieldType.iterable(Schema.FieldType.INT32.withNullable(true)))
            .build();

    Row row = Row.withSchema(schema).addValue(Arrays.asList(1, null)).build();
    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }

  @Test
  public void testConsistentWithEqualsMapWithNull() throws Exception {
    Schema schema =
        Schema.builder()
            .addField(
                "a",
                Schema.FieldType.map(
                    Schema.FieldType.INT32, Schema.FieldType.INT32.withNullable(true)))
            .build();

    Row row = Row.withSchema(schema).addValue(Collections.singletonMap(1, null)).build();
    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(schema), row);
  }
}
