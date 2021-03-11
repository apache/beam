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
package org.apache.beam.sdk.io.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-11436)
})
public class ThriftSchemaTest {
  private static final SchemaProvider defaultSchemaProvider = ThriftSchema.provider();
  private static final SchemaProvider customSchemaProvider =
      ThriftSchema.custom().typedef("StringSet", FieldType.iterable(FieldType.STRING)).provider();

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  @Test(expected = IllegalArgumentException.class)
  public void testThriftSchemaOnlyAllowsThriftClasses() {
    defaultSchemaProvider.schemaFor(TypeDescriptor.of(String.class));
  }

  @Test
  public void testInnerStructSchemaWithSimpleTypedefs() {
    // primitive typedefs don't need any special handling
    final Schema schema =
        defaultSchemaProvider.schemaFor(TypeDescriptor.of(TestThriftInnerStruct.class));
    assertNotNull(schema);
    assertEquals(TypeName.STRING, schema.getField("testNameTypedef").getType().getTypeName());
    assertEquals(TypeName.INT16, schema.getField("testAgeTypedef").getType().getTypeName());
  }

  @Test
  public void testUnionSchema() {
    final Schema schema = defaultSchemaProvider.schemaFor(TypeDescriptor.of(TestThriftUnion.class));
    assertNotNull(schema);
    assertEquals(TypeName.LOGICAL_TYPE, schema.getField("camelCaseEnum").getType().getTypeName());
    assertEquals(
        EnumerationType.IDENTIFIER,
        schema.getField("camelCaseEnum").getType().getLogicalType().getIdentifier());
    assertEquals(TypeName.ROW, schema.getField("snake_case_nested_struct").getType().getTypeName());
  }

  @Test(expected = IllegalStateException.class)
  public void testMainStructSchemaWithoutTypedefRegistration() {
    // container typedefs like set<string> cannot be inferred based on available metadata
    defaultSchemaProvider.schemaFor(TypeDescriptor.of(TestThriftStruct.class));
  }

  @Test
  public void testMainStructSchemaWithContainerTypedefRegistered() {
    final Schema schema = customSchemaProvider.schemaFor(TypeDescriptor.of(TestThriftStruct.class));
    assertNotNull(schema);
    assertEquals(TypeName.BOOLEAN, schema.getField("testBool").getType().getTypeName());
    assertEquals(TypeName.BYTE, schema.getField("testByte").getType().getTypeName());
    assertEquals(TypeName.INT16, schema.getField("testShort").getType().getTypeName());
    assertEquals(TypeName.INT32, schema.getField("testInt").getType().getTypeName());
    assertEquals(TypeName.INT64, schema.getField("testLong").getType().getTypeName());
    assertEquals(TypeName.DOUBLE, schema.getField("testDouble").getType().getTypeName());
    assertEquals(TypeName.BYTES, schema.getField("testBinary").getType().getTypeName());
    assertEquals(TypeName.MAP, schema.getField("stringIntMap").getType().getTypeName());
    assertEquals(TypeName.LOGICAL_TYPE, schema.getField("testEnum").getType().getTypeName());
    assertEquals(
        EnumerationType.IDENTIFIER,
        schema.getField("testEnum").getType().getLogicalType().getIdentifier());
    assertEquals(TypeName.ARRAY, schema.getField("testList").getType().getTypeName());
    assertEquals(TypeName.ROW, schema.getField("testNested").getType().getTypeName());
    assertEquals(
        TypeName.ITERABLE, schema.getField("testStringSetTypedef").getType().getTypeName());
  }

  @Test
  public void testSchemaUsage() {
    final List<TestThriftStruct> thriftData =
        Arrays.asList(
            thriftObj(1, 0.5, "k1", "k2"),
            thriftObj(2, 1.5, "k1", "k2"),
            thriftObj(1, 2.5, "k2", "k3"),
            thriftObj(2, 3.5, "k2", "k3"));

    testPipeline
        .getSchemaRegistry()
        .registerSchemaProvider(TestThriftStruct.class, customSchemaProvider);
    final PCollection<Row> rows =
        testPipeline.apply(Create.of(thriftData)).apply("toRows", Convert.toRows());

    playWithVariousDataTypes(rows);

    final PCollection<TestThriftStruct> restored =
        rows.apply("backToThrift", Convert.fromRows(TypeDescriptor.of(TestThriftStruct.class)));
    PAssert.that(restored).containsInAnyOrder(thriftData);

    testPipeline.run();
  }

  private void playWithVariousDataTypes(PCollection<Row> rows) {
    final PCollection<Row> sumByKey =
        rows.apply(
            Group.<Row>byFieldNames("testLong")
                .aggregateField("testDouble", Sum.ofDoubles(), "total"));
    final Schema keySchema = Schema.of(Field.of("key", FieldType.INT64));
    final Schema valueSchema = Schema.of(Field.of("value", FieldType.DOUBLE));
    PAssert.that(sumByKey)
        .containsInAnyOrder(
            Row.withSchema(sumByKey.getSchema())
                .addValues(
                    Row.withSchema(keySchema).addValues(1L).build(),
                    Row.withSchema(valueSchema).addValues(3.0).build())
                .build(),
            Row.withSchema(sumByKey.getSchema())
                .addValues(
                    Row.withSchema(keySchema).addValues(2L).build(),
                    Row.withSchema(valueSchema).addValues(5.0).build())
                .build());

    final PCollection<Long> count =
        rows.apply("bin", Select.fieldNames("testBinary"))
            .apply("distinctBin", Distinct.create())
            .apply(Count.globally());
    PAssert.that(count).containsInAnyOrder(2L);

    final PCollection<String> mapEntries =
        rows.apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        row ->
                            row.<String, Short>getMap("stringIntMap").keySet().stream()
                                .collect(Collectors.joining())))
            .apply("distinctMapEntries", Distinct.create());
    PAssert.that(mapEntries).containsInAnyOrder("k1k2", "k3k2");

    final PCollection<String> tags =
        rows.apply(
                FlatMapElements.into(TypeDescriptor.of(String.class))
                    .via(row -> row.getIterable("testStringSetTypedef")))
            .apply("distinctTags", Distinct.create());
    PAssert.that(tags).containsInAnyOrder("tag");

    final PCollection<Row> enumerated =
        rows.apply("enumSelect", Select.fieldNames("testEnum"))
            .apply("distinctEnumValues", Distinct.create());

    final Schema enumSchema =
        Schema.of(Field.of("testEnum", FieldType.logicalType(EnumerationType.create("C1", "C2"))));
    PAssert.that(enumerated)
        .containsInAnyOrder(
            Row.withSchema(enumSchema).addValues(new EnumerationType.Value(0)).build(),
            Row.withSchema(enumSchema).addValues(new EnumerationType.Value(1)).build());

    final PCollection<Row> unionNestedStructNames =
        rows.apply(
                "unionSelectStruct",
                Select.fieldNames("testUnion.snake_case_nested_struct.testNameTypedef"))
            .apply("distinctUnionNames", Distinct.create());

    final Schema unionNameSchema = Schema.of(Field.nullable("name", FieldType.STRING));
    PAssert.that(unionNestedStructNames)
        .containsInAnyOrder(
            Row.withSchema(unionNameSchema).addValues("kid").build(),
            Row.withSchema(unionNameSchema).addValues((String) null).build());

    final PCollection<Row> unionNestedEnumValues =
        rows.apply("unionSelectEnum", Select.fieldNames("testUnion.camelCaseEnum"))
            .apply("distinctUnionEnum", Distinct.create());

    final Schema unionNestedEnumSchema =
        Schema.of(
            Field.nullable("testEnum", FieldType.logicalType(EnumerationType.create("C1", "C2"))));
    PAssert.that(unionNestedEnumValues)
        .containsInAnyOrder(
            Row.withSchema(unionNestedEnumSchema).addValues(new EnumerationType.Value(0)).build(),
            Row.withSchema(unionNestedEnumSchema).addValues((EnumerationType.Value) null).build());

    final Schema nameSchema = Schema.of(Field.of("name", FieldType.STRING));
    final PCollection<Row> names =
        rows.apply("names", Select.fieldNames("testNested.testNameTypedef"))
            .apply("distinctNames", Distinct.create());
    PAssert.that(names)
        .containsInAnyOrder(Row.withSchema(nameSchema).addValues("Maradona").build());
  }

  private TestThriftStruct thriftObj(int index, double doubleValue, String... mapKeys) {
    final TestThriftStruct thrift = new TestThriftStruct();
    thrift.setTestLong(index);
    thrift.setTestInt(index);
    thrift.setTestDouble(doubleValue);
    final Map<String, Short> map =
        Stream.of(mapKeys).collect(Collectors.toMap(Function.identity(), k -> (short) k.length()));
    thrift.setStringIntMap(map);
    thrift.setTestBinary(String.join("", mapKeys).getBytes(StandardCharsets.UTF_8));
    thrift.setTestStringSetTypedef(Collections.singleton("tag"));
    thrift.setTestList(Arrays.asList(1, 2, 3));
    final TestThriftInnerStruct nested = new TestThriftInnerStruct("Maradona", (short) 60);
    thrift.setTestNested(nested);
    if (index % 2 == 0) {
      thrift.setTestEnum(TestThriftEnum.C1);
      thrift.setTestUnion(TestThriftUnion.snake_case_nested_struct(new TestThriftInnerStruct()));
    } else {
      thrift.setTestEnum(TestThriftEnum.C2);
      thrift.setTestUnion(TestThriftUnion.camelCaseEnum(TestThriftEnum.C1));
    }
    return thrift;
  }
}
