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
package org.apache.beam.sdk.extensions.sql.zetasql;

import static org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils.parseTimestampWithUTCTimeZone;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.zetasql.SqlException;
import com.google.zetasql.StructType;
import com.google.zetasql.StructType.StructField;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.Value;
import com.google.zetasql.ZetaSQLType.TypeKind;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for various operations/functions defined by ZetaSQL dialect. */
@RunWith(JUnit4.class)
public class ZetaSqlDialectSpecTest extends ZetaSqlTestBase {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    initialize();
  }

  @Test
  public void testSimpleSelect() {
    String sql =
        "SELECT CAST (1243 as INT64), "
            + "CAST ('2018-09-15 12:59:59.000000+00' as TIMESTAMP), "
            + "CAST ('string' as STRING);";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema =
        Schema.builder()
            .addInt64Field("field1")
            .addDateTimeField("field2")
            .addStringField("field3")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    1243L,
                    new DateTime(2018, 9, 15, 12, 59, 59, ISOChronology.getInstanceUTC()),
                    "string")
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // Verify that we can set the query planner via withQueryPlannerClass
  @Test
  public void testWithQueryPlannerClass() {
    String sql =
        "SELECT CAST (1243 as INT64), "
            + "CAST ('2018-09-15 12:59:59.000000+00' as TIMESTAMP), "
            + "CAST ('string' as STRING);";

    PCollection<Row> stream =
        pipeline.apply(SqlTransform.query(sql).withQueryPlannerClass(ZetaSQLQueryPlanner.class));
    final Schema schema =
        Schema.builder()
            .addInt64Field("field1")
            .addDateTimeField("field2")
            .addStringField("field3")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    1243L,
                    new DateTime(2018, 9, 15, 12, 59, 59, ISOChronology.getInstanceUTC()),
                    "string")
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // Verify that we can set the query planner via pipeline options
  @Test
  public void testPlannerNamePipelineOption() {
    pipeline
        .getOptions()
        .as(BeamSqlPipelineOptions.class)
        .setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner");

    String sql =
        "SELECT CAST (1243 as INT64), "
            + "CAST ('2018-09-15 12:59:59.000000+00' as TIMESTAMP), "
            + "CAST ('string' as STRING);";

    PCollection<Row> stream = pipeline.apply(SqlTransform.query(sql));
    final Schema schema =
        Schema.builder()
            .addInt64Field("field1")
            .addDateTimeField("field2")
            .addStringField("field3")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    1243L,
                    new DateTime(2018, 9, 15, 12, 59, 59, ISOChronology.getInstanceUTC()),
                    "string")
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testByteLiterals() {
    String sql = "SELECT b'abc'";

    byte[] byteString = new byte[] {'a', 'b', 'c'};

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("ColA", FieldType.BYTES).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(byteString).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testByteString() {
    String sql = "SELECT @p0 IS NULL AS ColA";

    ByteString byteString = ByteString.copyFrom(new byte[] {0x62});

    ImmutableMap<String, Value> params =
        ImmutableMap.<String, Value>builder().put("p0", Value.createBytesValue(byteString)).build();

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("ColA", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(false).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testFloat() {
    String sql = "SELECT 3.0";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("ColA", FieldType.DOUBLE).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(3.0).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testStringLiterals() {
    String sql = "SELECT '\"America/Los_Angeles\"\\n'";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("ColA", FieldType.STRING).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues("\"America/Los_Angeles\"\n").build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testParameterString() {
    String sql = "SELECT ?";
    ImmutableList<Value> params = ImmutableList.of(Value.createStringValue("abc\n"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("ColA", FieldType.STRING).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("abc\n").build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEQ1() {
    String sql = "SELECT @p0 = @p1 AS ColA";

    ImmutableMap<String, Value> params =
        ImmutableMap.<String, Value>builder()
            .put("p0", Value.createSimpleNullValue(TypeKind.TYPE_BOOL))
            .put("p1", Value.createBoolValue(true))
            .build();

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((Boolean) null).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEQ2() {
    String sql = "SELECT @p0 = @p1 AS ColA";

    ImmutableMap<String, Value> params =
        ImmutableMap.<String, Value>builder()
            .put("p0", Value.createDoubleValue(0))
            .put("p1", Value.createDoubleValue(Double.POSITIVE_INFINITY))
            .build();

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addBooleanField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(false).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEQ3() {
    String sql = "SELECT @p0 = @p1 AS ColA";

    ImmutableMap<String, Value> params =
        ImmutableMap.<String, Value>builder()
            .put("p0", Value.createSimpleNullValue(TypeKind.TYPE_DOUBLE))
            .put("p1", Value.createDoubleValue(3.14))
            .build();

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((Boolean) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEQ4() {
    String sql = "SELECT @p0 = @p1 AS ColA";

    ImmutableMap<String, Value> params =
        ImmutableMap.<String, Value>builder()
            .put("p0", Value.createBytesValue(ByteString.copyFromUtf8("hello")))
            .put("p1", Value.createBytesValue(ByteString.copyFromUtf8("hello")))
            .build();

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(true).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEQ5() {
    String sql = "SELECT b'hello' = b'hello' AS ColA";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(true).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEQ6() {
    String sql = "SELECT ? = ? AS ColA";
    ImmutableList<Value> params =
        ImmutableList.of(Value.createInt64Value(4L), Value.createInt64Value(5L));
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(false).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIsNotNull1() {
    String sql = "SELECT @p0 IS NOT NULL AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of("p0", Value.createSimpleNullValue(TypeKind.TYPE_STRING));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(false).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIsNotNull2() {
    String sql = "SELECT @p0 IS NOT NULL AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createNullValue(
                TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT64))));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(false).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIsNotNull3() {
    String sql = "SELECT @p0 IS NOT NULL AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createNullValue(
                TypeFactory.createStructType(
                    Arrays.asList(
                        new StructField(
                            "a", TypeFactory.createSimpleType(TypeKind.TYPE_STRING))))));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(false).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIfBasic() {
    String sql = "SELECT IF(@p0, @p1, @p2) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createBoolValue(true),
            "p1",
            Value.createInt64Value(1),
            "p2",
            Value.createInt64Value(2));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.INT64).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(1L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIfPositional() {
    String sql = "SELECT IF(?, ?, ?) AS ColA";

    ImmutableList<Value> params =
        ImmutableList.of(
            Value.createBoolValue(true), Value.createInt64Value(1), Value.createInt64Value(2));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.INT64).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(1L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCoalesceBasic() {
    String sql = "SELECT COALESCE(@p0, @p1, @p2) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createSimpleNullValue(TypeKind.TYPE_STRING),
            "p1",
            Value.createStringValue("yay"),
            "p2",
            Value.createStringValue("nay"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.STRING).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("yay").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCoalesceSingleArgument() {
    String sql = "SELECT COALESCE(@p0) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of("p0", Value.createSimpleNullValue(TypeKind.TYPE_INT64));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder().addNullableField("field1", FieldType.array(FieldType.INT64)).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValue(null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCoalesceNullArray() {
    String sql = "SELECT COALESCE(@p0, @p1) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createNullValue(
                TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT64))),
            "p1",
            Value.createNullValue(
                TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT64))));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder().addNullableField("field1", FieldType.array(FieldType.INT64)).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValue(null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testNullIfCoercion() {
    String sql = "SELECT NULLIF(@p0, @p1) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createInt64Value(3L),
            "p1",
            Value.createSimpleNullValue(TypeKind.TYPE_DOUBLE));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.DOUBLE).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValue(3.0).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCoalesceNullStruct() {
    String sql = "SELECT COALESCE(NULL, STRUCT(\"a\" AS s, -33 AS i))";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);

    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema innerSchema =
        Schema.of(Field.of("s", FieldType.STRING), Field.of("i", FieldType.INT64));
    final Schema schema =
        Schema.builder().addNullableField("field1", FieldType.row(innerSchema)).build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValue(Row.withSchema(innerSchema).addValues("a", -33L).build())
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIfTimestamp() {
    String sql = "SELECT IF(@p0, @p1, @p2) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createBoolValue(false),
            "p1",
            Value.createTimestampValueFromUnixMicros(0),
            "p2",
            Value.createTimestampValueFromUnixMicros(
                DateTime.parse("2019-01-01T00:00:00Z").getMillis() * 1000));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", DATETIME).build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(DateTime.parse("2019-01-01T00:00:00Z")).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore("$make_array is not implemented")
  public void testMakeArray() {
    String sql = "SELECT [s3, s1, s2] FROM (SELECT \"foo\" AS s1, \"bar\" AS s2, \"baz\" AS s3);";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);

    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder().addNullableField("field1", FieldType.array(FieldType.STRING)).build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValue(ImmutableList.of("baz", "foo", "bar")).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testNullIfPositive() {
    String sql = "SELECT NULLIF(@p0, @p1) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue("null"), "p1", Value.createStringValue("null"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.STRING).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValue(null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testNullIfNegative() {
    String sql = "SELECT NULLIF(@p0, @p1) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue("foo"), "p1", Value.createStringValue("null"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.STRING).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("foo").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIfNullPositive() {
    String sql = "SELECT IFNULL(@p0, @p1) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue("foo"), "p1", Value.createStringValue("default"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.STRING).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("foo").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIfNullNegative() {
    String sql = "SELECT IFNULL(@p0, @p1) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createSimpleNullValue(TypeKind.TYPE_STRING),
            "p1",
            Value.createStringValue("yay"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.STRING).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("yay").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEmptyArrayParameter() {
    String sql = "SELECT @p0 AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createArrayValue(
                TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
                ImmutableList.of()));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addArrayField("field1", FieldType.INT64).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValue(ImmutableList.of()).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEmptyArrayLiteral() {
    String sql = "SELECT ARRAY<STRING>[];";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addArrayField("field1", FieldType.STRING).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValue(ImmutableList.of()).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLike1() {
    String sql = "SELECT @p0 LIKE @p1 AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue("ab%"), "p1", Value.createStringValue("ab\\%"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(true).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLikeNullPattern() {
    String sql = "SELECT @p0 LIKE @p1 AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createStringValue("ab%"),
            "p1",
            Value.createSimpleNullValue(TypeKind.TYPE_STRING));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((Object) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLikeAllowsEscapingNonSpecialCharacter() {
    String sql = "SELECT @p0 LIKE  @p1 AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of("p0", Value.createStringValue("ab"), "p1", Value.createStringValue("\\ab"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(true).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLikeAllowsEscapingBackslash() {
    String sql = "SELECT @p0 LIKE  @p1 AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue("a\\c"), "p1", Value.createStringValue("a\\\\c"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(true).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLikeBytes() {
    String sql = "SELECT @p0 LIKE  @p1 AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createBytesValue(ByteString.copyFromUtf8("abcd")),
            "p1",
            Value.createBytesValue(ByteString.copyFromUtf8("__%")));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(true).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testMod() {
    String sql = "SELECT MOD(4, 2)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(0L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSimpleUnionAll() {
    String sql =
        "SELECT CAST (1243 as INT64), "
            + "CAST ('2018-09-15 12:59:59.000000+00' as TIMESTAMP), "
            + "CAST ('string' as STRING) "
            + " UNION ALL "
            + " SELECT CAST (1243 as INT64), "
            + "CAST ('2018-09-15 12:59:59.000000+00' as TIMESTAMP), "
            + "CAST ('string' as STRING);";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("field1")
            .addDateTimeField("field2")
            .addStringField("field3")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    1243L,
                    new DateTime(2018, 9, 15, 12, 59, 59, ISOChronology.getInstanceUTC()),
                    "string")
                .build(),
            Row.withSchema(schema)
                .addValues(
                    1243L,
                    new DateTime(2018, 9, 15, 12, 59, 59, ISOChronology.getInstanceUTC()),
                    "string")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testThreeWayUnionAll() {
    String sql = "SELECT a FROM (SELECT 1 a UNION ALL SELECT 2 UNION ALL SELECT 3)";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(1L).build(),
            Row.withSchema(schema).addValues(2L).build(),
            Row.withSchema(schema).addValues(3L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSimpleUnionDISTINCT() {
    String sql =
        "SELECT CAST (1243 as INT64), "
            + "CAST ('2018-09-15 12:59:59.000000+00' as TIMESTAMP), "
            + "CAST ('string' as STRING) "
            + " UNION DISTINCT "
            + " SELECT CAST (1243 as INT64), "
            + "CAST ('2018-09-15 12:59:59.000000+00' as TIMESTAMP), "
            + "CAST ('string' as STRING);";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("field1")
            .addDateTimeField("field2")
            .addStringField("field3")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    1243L,
                    new DateTime(2018, 9, 15, 12, 59, 59, ISOChronology.getInstanceUTC()),
                    "string")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLInnerJoin() {
    String sql =
        "SELECT t1.Key "
            + "FROM KeyValue AS t1"
            + " INNER JOIN BigTable AS t2"
            + " on "
            + " t1.Key = t2.RowKey AND t1.ts = t2.ts";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("field1").build())
                .addValues(15L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  // JOIN USING(col) is equivalent to JOIN on left.col = right.col.
  public void testZetaSQLInnerJoinWithUsing() {
    String sql = "SELECT t1.Key " + "FROM KeyValue AS t1" + " INNER JOIN BigTable AS t2 USING(ts)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("field1").build())
                .addValues(15L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  // testing ordering of the JOIN conditions.
  public void testZetaSQLInnerJoinTwo() {
    String sql =
        "SELECT t2.RowKey "
            + "FROM KeyValue AS t1"
            + " INNER JOIN BigTable AS t2"
            + " on "
            + " t2.RowKey = t1.Key AND t2.ts = t1.ts";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("field1").build())
                .addValues(15L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLLeftOuterJoin() {
    String sql =
        "SELECT * "
            + "FROM KeyValue AS t1"
            + " LEFT JOIN BigTable AS t2"
            + " on "
            + " t1.Key = t2.RowKey";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schemaOne =
        Schema.builder()
            .addInt64Field("field1")
            .addStringField("field2")
            .addDateTimeField("field3")
            .addNullableField("field4", FieldType.INT64)
            .addNullableField("field5", FieldType.STRING)
            .addNullableField("field6", DATETIME)
            .build();

    final Schema schemaTwo =
        Schema.builder()
            .addInt64Field("field1")
            .addStringField("field2")
            .addDateTimeField("field3")
            .addInt64Field("field4")
            .addStringField("field5")
            .addDateTimeField("field6")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schemaOne)
                .addValues(
                    14L,
                    "KeyValue234",
                    new DateTime(2018, 7, 1, 21, 26, 6, ISOChronology.getInstanceUTC()),
                    null,
                    null,
                    null)
                .build(),
            Row.withSchema(schemaTwo)
                .addValues(
                    15L,
                    "KeyValue235",
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()),
                    15L,
                    "BigTable235",
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLRightOuterJoin() {
    String sql =
        "SELECT * "
            + "FROM KeyValue AS t1"
            + " RIGHT JOIN BigTable AS t2"
            + " on "
            + " t1.Key = t2.RowKey";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schemaOne =
        Schema.builder()
            .addNullableField("field1", FieldType.INT64)
            .addNullableField("field2", FieldType.STRING)
            .addNullableField("field3", DATETIME)
            .addInt64Field("field4")
            .addStringField("field5")
            .addDateTimeField("field6")
            .build();

    final Schema schemaTwo =
        Schema.builder()
            .addInt64Field("field1")
            .addStringField("field2")
            .addDateTimeField("field3")
            .addInt64Field("field4")
            .addStringField("field5")
            .addDateTimeField("field6")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schemaOne)
                .addValues(
                    null,
                    null,
                    null,
                    16L,
                    "BigTable236",
                    new DateTime(2018, 7, 1, 21, 26, 8, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schemaTwo)
                .addValues(
                    15L,
                    "KeyValue235",
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()),
                    15L,
                    "BigTable235",
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLFullOuterJoin() {
    String sql =
        "SELECT * "
            + "FROM KeyValue AS t1"
            + " FULL JOIN BigTable AS t2"
            + " on "
            + " t1.Key = t2.RowKey";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schemaOne =
        Schema.builder()
            .addNullableField("field1", FieldType.INT64)
            .addNullableField("field2", FieldType.STRING)
            .addNullableField("field3", DATETIME)
            .addInt64Field("field4")
            .addStringField("field5")
            .addDateTimeField("field6")
            .build();

    final Schema schemaTwo =
        Schema.builder()
            .addInt64Field("field1")
            .addStringField("field2")
            .addDateTimeField("field3")
            .addInt64Field("field4")
            .addStringField("field5")
            .addDateTimeField("field6")
            .build();

    final Schema schemaThree =
        Schema.builder()
            .addInt64Field("field1")
            .addStringField("field2")
            .addDateTimeField("field3")
            .addNullableField("field4", FieldType.INT64)
            .addNullableField("field5", FieldType.STRING)
            .addNullableField("field6", DATETIME)
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schemaOne)
                .addValues(
                    null,
                    null,
                    null,
                    16L,
                    "BigTable236",
                    new DateTime(2018, 7, 1, 21, 26, 8, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schemaTwo)
                .addValues(
                    15L,
                    "KeyValue235",
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()),
                    15L,
                    "BigTable235",
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schemaThree)
                .addValues(
                    14L,
                    "KeyValue234",
                    new DateTime(2018, 7, 1, 21, 26, 6, ISOChronology.getInstanceUTC()),
                    null,
                    null,
                    null)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore("BeamSQL only supports equal join")
  public void testZetaSQLFullOuterJoinTwo() {
    String sql =
        "SELECT * "
            + "FROM KeyValue AS t1"
            + " FULL JOIN BigTable AS t2"
            + " on "
            + " t1.Key + t2.RowKey = 30";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLFullOuterJoinFalse() {
    String sql = "SELECT * FROM KeyValue AS t1 FULL JOIN BigTable AS t2 ON false";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    thrown.expect(UnsupportedOperationException.class);
    BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
  }

  @Test
  public void testZetaSQLThreeWayInnerJoin() {
    String sql =
        "SELECT t3.Value, t2.Value, t1.Value, t1.Key, t3.ColId FROM KeyValue as t1 "
            + "JOIN BigTable as t2 "
            + "ON (t1.Key = t2.RowKey) "
            + "JOIN Spanner as t3 "
            + "ON (t3.ColId = t1.Key)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addStringField("t3.Value")
                        .addStringField("t2.Value")
                        .addStringField("t1.Value")
                        .addInt64Field("t1.Key")
                        .addInt64Field("t3.ColId")
                        .build())
                .addValues("Spanner235", "BigTable235", "KeyValue235", 15L, 15L)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLTableJoinOnItselfWithFiltering() {
    String sql =
        "SELECT * FROM Spanner as t1 "
            + "JOIN Spanner as t2 "
            + "ON (t1.ColId = t2.ColId) WHERE t1.ColId = 17";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addInt64Field("field1")
                        .addStringField("field2")
                        .addInt64Field("field3")
                        .addStringField("field4")
                        .build())
                .addValues(17L, "Spanner237", 17L, "Spanner237")
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLSelectFromSelect() {
    String sql = "SELECT * FROM (SELECT \"apple\" AS fruit, \"carrot\" AS vegetable);";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder().addStringField("field1").addStringField("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues("apple", "carrot").build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));

    Schema outputSchema = stream.getSchema();
    Assert.assertEquals(2, outputSchema.getFieldCount());
    Assert.assertEquals("fruit", outputSchema.getField(0).getName());
    Assert.assertEquals("vegetable", outputSchema.getField(1).getName());
  }

  @Test
  public void testZetaSQLSelectFromTable() {
    String sql = "SELECT Key, Value FROM KeyValue;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").addStringField("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(14L, "KeyValue234").build(),
            Row.withSchema(schema).addValues(15L, "KeyValue235").build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLSelectFromTableLimit() {
    String sql = "SELECT Key, Value FROM KeyValue LIMIT 2;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").addStringField("field2").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(14L, "KeyValue234").build(),
            Row.withSchema(schema).addValues(15L, "KeyValue235").build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLSelectFromTableLimit0() {
    String sql = "SELECT Key, Value FROM KeyValue LIMIT 0;";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    PAssert.that(stream).containsInAnyOrder();
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLSelectNullLimitParam() {
    String sql = "SELECT Key, Value FROM KeyValue LIMIT @lmt;";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "lmt", Value.createNullValue(TypeFactory.createSimpleType(TypeKind.TYPE_INT64)));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Limit requires non-null count and offset");
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
  }

  @Test
  public void testZetaSQLSelectNullOffsetParam() {
    String sql = "SELECT Key, Value FROM KeyValue LIMIT 1 OFFSET @lmt;";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "lmt", Value.createNullValue(TypeFactory.createSimpleType(TypeKind.TYPE_INT64)));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Limit requires non-null count and offset");
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
  }

  @Test
  public void testZetaSQLSelectFromTableOrderLimit() {
    String sql =
        "SELECT x, y FROM (SELECT 1 as x, 0 as y UNION ALL SELECT 0, 0 "
            + "UNION ALL SELECT 1, 0 UNION ALL SELECT 1, 1) ORDER BY x LIMIT 1";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addInt64Field("field1").addInt64Field("field2").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(0L, 0L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLSelectFromTableLimitOffset() {
    String sql =
        "SELECT COUNT(a) FROM (\n"
            + "SELECT a FROM (SELECT 1 a UNION ALL SELECT 2 UNION ALL SELECT 3) LIMIT 3 OFFSET 1);";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addInt64Field("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(2L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // There is really no order for a PCollection, so this query does not test
  // ORDER BY but just a test to see if ORDER BY LIMIT can work.
  @Test
  public void testZetaSQLSelectFromTableOrderByLimit() {
    String sql = "SELECT Key, Value FROM KeyValue ORDER BY Key DESC LIMIT 2;";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").addStringField("field2").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(14L, "KeyValue234").build(),
            Row.withSchema(schema).addValues(15L, "KeyValue235").build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLSelectFromTableOrderBy() {
    String sql = "SELECT Key, Value FROM KeyValue ORDER BY Key DESC;";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("ORDER BY without a LIMIT is not supported.");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testZetaSQLSelectFromTableWithStructType2() {
    String sql =
        "SELECT table_with_struct.struct_col.struct_col_str FROM table_with_struct WHERE id = 1;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addStringField("field").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValue("row_one").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLStructFieldAccessInFilter() {
    String sql =
        "SELECT table_with_struct.id FROM table_with_struct WHERE"
            + " table_with_struct.struct_col.struct_col_str = 'row_one';";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addInt64Field("field").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValue(1L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLStructFieldAccessInCast() {
    String sql =
        "SELECT CAST(table_with_struct.id AS STRING) FROM table_with_struct WHERE"
            + " table_with_struct.struct_col.struct_col_str = 'row_one';";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addStringField("field").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValue("1").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore("[BEAM-9191] CAST operator does not work fully due to bugs in unparsing")
  public void testZetaSQLStructFieldAccessInCast2() {
    String sql =
        "SELECT CAST(A.struct_col.struct_col_str AS TIMESTAMP) FROM table_with_struct_ts_string AS"
            + " A";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addDateTimeField("field").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValue(parseTimestampWithUTCTimeZone("2019-01-15 13:21:03"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  // Used to validate fix for [BEAM-8042].
  public void testAggregateWithAndWithoutColumnRefs() {
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);

    String sql =
        "SELECT \n"
            + "  id, \n"
            + "  SUM(has_f1) as f1_count, \n"
            + "  SUM(has_f2) as f2_count, \n"
            + "  SUM(has_f3) as f3_count, \n"
            + "  SUM(has_f4) as f4_count, \n"
            + "  SUM(has_f5) as f5_count, \n"
            + "  COUNT(*) as count, \n"
            + "  SUM(has_f6) as f6_count  \n"
            + "FROM (select 0 as id, 1 as has_f1, 2 as has_f2, 3 as has_f3, 4 as has_f4, 5 as has_f5, 6 as has_f6)\n"
            + "GROUP BY id";

    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema =
        Schema.builder()
            .addInt64Field("id")
            .addInt64Field("f1_count")
            .addInt64Field("f2_count")
            .addInt64Field("f3_count")
            .addInt64Field("f4_count")
            .addInt64Field("f5_count")
            .addInt64Field("count")
            .addInt64Field("f6_count")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(0L, 1L, 2L, 3L, 4L, 5L, 1L, 6L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLStructFieldAccessInGroupBy() {
    String sql = "SELECT rowCol.row_id, COUNT(*) FROM table_with_struct_two GROUP BY rowCol.row_id";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addInt64Field("field1").addInt64Field("field2").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(1L, 1L).build(),
            Row.withSchema(schema).addValues(2L, 1L).build(),
            Row.withSchema(schema).addValues(3L, 2L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  // TODO: Fix Later
  @SuppressWarnings("nullness")
  public void testZetaSQLAnyValueInGroupBy() {
    String sql =
        "SELECT rowCol.row_id as key, ANY_VALUE(rowCol.data) as any_value FROM table_with_struct_two GROUP BY rowCol.row_id";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Map<Long, List<String>> allowedTuples = new HashMap<>();
    allowedTuples.put(1L, Arrays.asList("data1"));
    allowedTuples.put(2L, Arrays.asList("data2"));
    allowedTuples.put(3L, Arrays.asList("data2", "data3"));

    PAssert.that(stream)
        .satisfies(
            input -> {
              Iterator<Row> iter = input.iterator();
              while (iter.hasNext()) {
                Row row = iter.next();
                List<String> values = allowedTuples.remove(row.getInt64("key"));
                assertTrue(values != null);
                assertTrue(values.contains(row.getString("any_value")));
              }
              assertTrue(allowedTuples.isEmpty());
              return null;
            });

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLStructFieldAccessInGroupBy2() {
    String sql =
        "SELECT rowCol.data, MAX(rowCol.row_id), MIN(rowCol.row_id) FROM table_with_struct_two"
            + " GROUP BY rowCol.data";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema =
        Schema.builder()
            .addStringField("field1")
            .addInt64Field("field2")
            .addInt64Field("field3")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("data1", 1L, 1L).build(),
            Row.withSchema(schema).addValues("data2", 3L, 2L).build(),
            Row.withSchema(schema).addValues("data3", 3L, 3L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLStructFieldAccessInnerJoin() {
    String sql =
        "SELECT A.rowCol.data FROM table_with_struct_two AS A INNER JOIN "
            + "table_with_struct AS B "
            + "ON A.rowCol.row_id = B.id";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addStringField("field1").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValue("data1").build(),
            Row.withSchema(schema).addValue("data2").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLSelectFromTableWithArrayType() {
    String sql = "SELECT array_col FROM table_with_array;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addArrayField("field", FieldType.STRING).build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValue(Arrays.asList("1", "2", "3")).build(),
            Row.withSchema(schema).addValue(ImmutableList.of()).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLSelectStarFromTable() {
    String sql = "SELECT * FROM BigTable;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("field1")
            .addStringField("field2")
            .addDateTimeField("field3")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    15L,
                    "BigTable235",
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    16L,
                    "BigTable236",
                    new DateTime(2018, 7, 1, 21, 26, 8, ISOChronology.getInstanceUTC()))
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLBasicFiltering() {
    String sql = "SELECT Key, Value FROM KeyValue WHERE Key = 14;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addInt64Field("field1").addStringField("field2").build())
                .addValues(14L, "KeyValue234")
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLBasicFilteringTwo() {
    String sql = "SELECT Key, Value FROM KeyValue WHERE Key = 14 AND Value = 'non-existing';";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    PAssert.that(stream).containsInAnyOrder();

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLBasicFilteringThree() {
    String sql = "SELECT Key, Value FROM KeyValue WHERE Key = 14 OR Key = 15;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").addStringField("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(14L, "KeyValue234").build(),
            Row.withSchema(schema).addValues(15L, "KeyValue235").build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLCountOnAColumn() {
    String sql = "SELECT COUNT(Key) FROM KeyValue";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(2L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLAggDistinct() {
    String sql = "SELECT Key, COUNT(DISTINCT Value) FROM KeyValue GROUP BY Key";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Does not support COUNT DISTINCT");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testZetaSQLBasicAgg() {
    String sql = "SELECT Key, COUNT(*) FROM KeyValue GROUP BY Key";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").addInt64Field("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(14L, 1L).build(),
            Row.withSchema(schema).addValues(15L, 1L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLColumnAlias1() {
    String sql = "SELECT Key, COUNT(*) AS count_col FROM KeyValue GROUP BY Key";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));

    Schema outputSchema = stream.getSchema();
    Assert.assertEquals(2, outputSchema.getFieldCount());
    Assert.assertEquals("Key", outputSchema.getField(0).getName());
    Assert.assertEquals("count_col", outputSchema.getField(1).getName());
  }

  @Test
  public void testZetaSQLColumnAlias2() {
    String sql =
        "SELECT Key AS k1, (count_col + 1) AS k2 FROM (SELECT Key, COUNT(*) AS count_col FROM"
            + " KeyValue GROUP BY Key)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));

    Schema outputSchema = stream.getSchema();
    Assert.assertEquals(2, outputSchema.getFieldCount());
    Assert.assertEquals("k1", outputSchema.getField(0).getName());
    Assert.assertEquals("k2", outputSchema.getField(1).getName());
  }

  @Test
  public void testZetaSQLColumnAlias3() {
    String sql = "SELECT Key AS v1, Value AS v2, ts AS v3 FROM KeyValue";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));

    Schema outputSchema = stream.getSchema();
    Assert.assertEquals(3, outputSchema.getFieldCount());
    Assert.assertEquals("v1", outputSchema.getField(0).getName());
    Assert.assertEquals("v2", outputSchema.getField(1).getName());
    Assert.assertEquals("v3", outputSchema.getField(2).getName());
  }

  @Test
  public void testZetaSQLColumnAlias4() {
    String sql = "SELECT CAST(123 AS INT64) AS cast_col";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));

    Schema outputSchema = stream.getSchema();
    Assert.assertEquals(1, outputSchema.getFieldCount());
    Assert.assertEquals("cast_col", outputSchema.getField(0).getName());
  }

  @Test
  public void testZetaSQLAmbiguousAlias() {
    String sql = "SELECT row_id as ID, int64_col as ID FROM table_all_types GROUP BY ID;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);

    thrown.expectMessage(
        "Name ID in GROUP BY clause is ambiguous; it may refer to multiple columns in the"
            + " SELECT-list [at 1:68]");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testZetaSQLAggWithOrdinalReference() {
    String sql = "SELECT Key, COUNT(*) FROM aggregate_test_table GROUP BY 1";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").addInt64Field("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(1L, 2L).build(),
            Row.withSchema(schema).addValues(2L, 3L).build(),
            Row.withSchema(schema).addValues(3L, 2L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLAggWithAliasReference() {
    String sql = "SELECT Key AS K, COUNT(*) FROM aggregate_test_table GROUP BY K";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").addInt64Field("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(1L, 2L).build(),
            Row.withSchema(schema).addValues(2L, 3L).build(),
            Row.withSchema(schema).addValues(3L, 2L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLBasicAgg2() {
    String sql = "SELECT Key, COUNT(*) FROM aggregate_test_table GROUP BY Key";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").addInt64Field("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(1L, 2L).build(),
            Row.withSchema(schema).addValues(2L, 3L).build(),
            Row.withSchema(schema).addValues(3L, 2L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLBasicAgg3() {
    String sql = "SELECT Key, Key2, COUNT(*) FROM aggregate_test_table GROUP BY Key2, Key";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("field1")
            .addInt64Field("field3")
            .addInt64Field("field2")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(1L, 10L, 1L).build(),
            Row.withSchema(schema).addValues(1L, 11L, 1L).build(),
            Row.withSchema(schema).addValues(2L, 11L, 2L).build(),
            Row.withSchema(schema).addValues(2L, 12L, 1L).build(),
            Row.withSchema(schema).addValues(3L, 13L, 2L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLBasicAgg4() {
    String sql =
        "SELECT Key, Key2, MAX(f_int_1), MIN(f_int_1), SUM(f_int_1), SUM(f_double_1) "
            + "FROM aggregate_test_table GROUP BY Key2, Key";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("field1")
            .addInt64Field("field3")
            .addInt64Field("field2")
            .addInt64Field("field4")
            .addInt64Field("field5")
            .addDoubleField("field6")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(1L, 10L, 1L, 1L, 1L, 1.0).build(),
            Row.withSchema(schema).addValues(1L, 11L, 2L, 2L, 2L, 2.0).build(),
            Row.withSchema(schema).addValues(2L, 11L, 4L, 3L, 7L, 7.0).build(),
            Row.withSchema(schema).addValues(2L, 12L, 5L, 5L, 5L, 5.0).build(),
            Row.withSchema(schema).addValues(3L, 13L, 7L, 6L, 13L, 13.0).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLBasicAgg5() {
    String sql =
        "SELECT Key, Key2, AVG(CAST(f_int_1 AS FLOAT64)), AVG(f_double_1) "
            + "FROM aggregate_test_table GROUP BY Key2, Key";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("field1")
            .addInt64Field("field2")
            .addDoubleField("field3")
            .addDoubleField("field4")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(1L, 10L, 1.0, 1.0).build(),
            Row.withSchema(schema).addValues(1L, 11L, 2.0, 2.0).build(),
            Row.withSchema(schema).addValues(2L, 11L, 3.5, 3.5).build(),
            Row.withSchema(schema).addValues(2L, 12L, 5.0, 5.0).build(),
            Row.withSchema(schema).addValues(3L, 13L, 6.5, 6.5).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore(
      "Calcite infers return type of AVG(int64) as BIGINT while ZetaSQL requires it as either"
          + " NUMERIC or DOUBLE/FLOAT64")
  public void testZetaSQLTestAVG() {
    String sql = "SELECT Key, AVG(f_int_1)" + "FROM aggregate_test_table GROUP BY Key";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("field1")
            .addInt64Field("field2")
            .addInt64Field("field3")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(1L, 10L, 1L).build(),
            Row.withSchema(schema).addValues(1L, 11L, 6L).build(),
            Row.withSchema(schema).addValues(2L, 11L, 6L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLGroupByExprInSelect() {
    String sql = "SELECT int64_col + 1 FROM table_all_types GROUP BY int64_col + 1;";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValue(0L).build(),
            Row.withSchema(schema).addValue(-1L).build(),
            Row.withSchema(schema).addValue(-2L).build(),
            Row.withSchema(schema).addValue(-3L).build(),
            Row.withSchema(schema).addValue(-4L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLGroupByAndFiltering() {
    String sql = "SELECT int64_col FROM table_all_types WHERE int64_col = 1 GROUP BY int64_col;";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    PAssert.that(stream).containsInAnyOrder();
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLGroupByAndFilteringOnNonGroupByColumn() {
    String sql = "SELECT int64_col FROM table_all_types WHERE double_col = 0.5 GROUP BY int64_col;";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addInt64Field("field").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValue(-5L).build(),
            Row.withSchema(schema).addValue(-4L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLBasicHaving() {
    String sql = "SELECT Key, COUNT(*) FROM aggregate_test_table GROUP BY Key HAVING COUNT(*) > 2";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").addInt64Field("field2").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(2L, 3L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLHavingNull() {
    String sql = "SELECT SUM(int64_val) FROM all_null_table GROUP BY primary_key HAVING false";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field").build();

    PAssert.that(stream).empty();

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLBasicFixedWindowing() {
    String sql =
        "SELECT "
            + "COUNT(*) as field_count, "
            + "TUMBLE_START(\"INTERVAL 1 SECOND\") as window_start, "
            + "TUMBLE_END(\"INTERVAL 1 SECOND\") as window_end "
            + "FROM KeyValue "
            + "GROUP BY TUMBLE(ts, \"INTERVAL 1 SECOND\");";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("count_start")
            .addDateTimeField("field1")
            .addDateTimeField("field2")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    1L,
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 8, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    1L,
                    new DateTime(2018, 7, 1, 21, 26, 6, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // Test nested selection
  @Test
  public void testZetaSQLNestedQueryOne() {
    String sql =
        "SELECT a.Value, a.Key FROM (SELECT Key, Value FROM KeyValue WHERE Key = 14 OR Key = 15)"
            + " as a;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field2").addInt64Field("field1").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("KeyValue234", 14L).build(),
            Row.withSchema(schema).addValues("KeyValue235", 15L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // Test selection, filtering and aggregation combined query.
  @Test
  public void testZetaSQLNestedQueryTwo() {
    String sql =
        "SELECT a.Key, a.Key2, COUNT(*) FROM "
            + " (SELECT * FROM aggregate_test_table WHERE Key != 10) as a "
            + " GROUP BY a.Key2, a.Key";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("field1")
            .addInt64Field("field3")
            .addInt64Field("field2")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(1L, 10L, 1L).build(),
            Row.withSchema(schema).addValues(1L, 11L, 1L).build(),
            Row.withSchema(schema).addValues(2L, 11L, 2L).build(),
            Row.withSchema(schema).addValues(2L, 12L, 1L).build(),
            Row.withSchema(schema).addValues(3L, 13L, 2L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // test selection and join combined query
  @Test
  public void testZetaSQLNestedQueryThree() {
    String sql =
        "SELECT * FROM (SELECT * FROM KeyValue) AS t1 INNER JOIN (SELECT * FROM BigTable) AS t2 on"
            + " t1.Key = t2.RowKey";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addInt64Field("Key")
                        .addStringField("Value")
                        .addDateTimeField("ts")
                        .addInt64Field("RowKey")
                        .addStringField("Value2")
                        .addDateTimeField("ts2")
                        .build())
                .addValues(
                    15L,
                    "KeyValue235",
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()),
                    15L,
                    "BigTable235",
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // Test nested select with out of order columns.
  @Test
  public void testZetaSQLNestedQueryFive() {
    String sql =
        "SELECT a.Value, a.Key FROM (SELECT Value, Key FROM KeyValue WHERE Key = 14 OR Key = 15)"
            + " as a;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field2").addInt64Field("field1").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("KeyValue234", 14L).build(),
            Row.withSchema(schema).addValues("KeyValue235", 15L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  /////////////////////////////////////////////////////////////////////////////
  // DOUBLE INF and NAN tests
  /////////////////////////////////////////////////////////////////////////////

  @Test
  public void testDoubleINF() {
    String sql = "SELECT CAST('+inf' AS FLOAT64), CAST('-inf' AS FLOAT64)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDoubleField("f_double1")
                        .addDoubleField("f_double2")
                        .build())
                .addValues(Double.POSITIVE_INFINITY)
                .addValues(Double.NEGATIVE_INFINITY)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDoubleINFEQ() {
    String sql =
        "SELECT CAST('+inf' AS FLOAT64) = CAST('+inf' AS FLOAT64), CAST('+inf' AS FLOAT64) = CAST('-inf' AS FLOAT64)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addBooleanField("f_boolean1")
                        .addBooleanField("f_boolean2")
                        .build())
                .addValues(true)
                .addValues(false)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDoubleNAN() {
    String sql = "SELECT CAST('NaN' AS FLOAT64)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDoubleField("f_double").build())
                .addValues(Double.NaN)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDoubleNaNEQ() {
    String sql = "SELECT CAST('NaN' AS FLOAT64) = CAST('NaN' AS FLOAT64)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addBooleanField("f_boolean").build())
                .addValues(false)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDoubleIsINF() {
    String sql =
        "SELECT IS_INF(CAST('+inf' AS FLOAT64)), IS_INF(CAST('-inf' AS FLOAT64)), IS_INF(3.0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addBooleanField("f_boolean1")
                        .addBooleanField("f_boolean2")
                        .addBooleanField("f_boolean3")
                        .build())
                .addValues(true)
                .addValues(true)
                .addValues(false)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDoubleIsNAN() {
    String sql = "SELECT IS_NAN(CAST('NaN' AS FLOAT64)), IS_NAN(3.0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addBooleanField("f_boolean1")
                        .addBooleanField("f_boolean2")
                        .build())
                .addValues(true)
                .addValues(false)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  /////////////////////////////////////////////////////////////////////////////
  // NUMERIC type tests
  /////////////////////////////////////////////////////////////////////////////

  @Test
  public void testNumericLiteral() {
    String sql =
        "SELECT NUMERIC '0', "
            + "NUMERIC '123456', "
            + "NUMERIC '-3.14', "
            + "NUMERIC '-0.54321', "
            + "NUMERIC '1.23456e05', "
            + "NUMERIC '-9.876e-3', "
            // min value for ZetaSQL NUMERIC type
            + "NUMERIC '-99999999999999999999999999999.999999999', "
            // max value for ZetaSQL NUMERIC type
            + "NUMERIC '99999999999999999999999999999.999999999'";
    ;

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDecimalField("f_numeric1")
                        .addDecimalField("f_numeric2")
                        .addDecimalField("f_numeric3")
                        .addDecimalField("f_numeric4")
                        .addDecimalField("f_numeric5")
                        .addDecimalField("f_numeric6")
                        .addDecimalField("f_numeric7")
                        .addDecimalField("f_numeric8")
                        .build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("0"))
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("123456"))
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("-3.14"))
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("-0.54321"))
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("123456"))
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("-0.009876"))
                .addValues(
                    ZetaSqlTypesUtils.bigDecimalAsNumeric(
                        "-99999999999999999999999999999.999999999"))
                .addValues(
                    ZetaSqlTypesUtils.bigDecimalAsNumeric(
                        "99999999999999999999999999999.999999999"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testNumericColumn() {
    String sql = "SELECT numeric_field FROM table_with_numeric";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("123.4567"))
                .build(),
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("765.4321"))
                .build(),
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("-555.5555"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUnaryMinusNumeric() {
    String sql = "SELECT - NUMERIC '1.23456e05'";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("-123456"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testAddNumeric() {
    String sql = "SELECT NUMERIC '1.23456e05' + NUMERIC '9.876e-3'";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("123456.009876"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSubNumeric() {
    String sql = "SELECT NUMERIC '1.23456e05' - NUMERIC '-9.876e-3'";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("123456.009876"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testMultiNumeric() {
    String sql = "SELECT NUMERIC '1.23e02' * NUMERIC '-1.001e-3'";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("-0.123123"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDivNumeric() {
    String sql = "SELECT NUMERIC '-1.23123e-1' / NUMERIC '-1.001e-3'";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("123"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testModNumeric() {
    String sql = "SELECT MOD(NUMERIC '1.23456e05', NUMERIC '5')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("1"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testFloorNumeric() {
    String sql = "SELECT FLOOR(NUMERIC '1.23456e04'), FLOOR(NUMERIC '-1.23456e04')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDecimalField("f_numeric1")
                        .addDecimalField("f_numeric2")
                        .build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("12345"))
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("-12346"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCeilNumeric() {
    String sql = "SELECT CEIL(NUMERIC '1.23456e04'), CEIL(NUMERIC '-1.23456e04')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDecimalField("f_numeric1")
                        .addDecimalField("f_numeric2")
                        .build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("12346"))
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("-12345"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore("https://jira.apache.org/jira/browse/BEAM-10459")
  public void testSumNumeric() {
    String sql = "SELECT SUM(numeric_field) FROM table_with_numeric";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("333.3333"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore("https://jira.apache.org/jira/browse/BEAM-10459")
  public void testAvgNumeric() {
    String sql = "SELECT AVG(numeric_field) FROM table_with_numeric";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("111.1111"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testMultipleSelectStatementsThrowsException() {
    String sql = "SELECT 1; SELECT 2;";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("No additional statements are allowed after a SELECT statement.");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testAlreadyDefinedUDFThrowsException() {
    String sql = "CREATE FUNCTION foo() AS (0); CREATE FUNCTION foo() AS (1); SELECT foo();";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(ParseException.class);
    thrown.expectMessage("Failed to define function foo");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testCreateFunctionNoSelectThrowsException() {
    String sql = "CREATE FUNCTION plusOne(x INT64) AS (x + 1);";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Statement list must end in a SELECT statement, not CreateFunctionStmt");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testNullaryUdf() {
    String sql = "CREATE FUNCTION zero() AS (0); SELECT zero();";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("x").build()).addValue(0L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testQualifiedNameUdfUnqualifiedCall() {
    String sql = "CREATE FUNCTION foo.bar.baz() AS (\"uwu\"); SELECT baz();";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("x").build()).addValue("uwu").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore(
      "Qualified paths can't be resolved due to a bug in ZetaSQL: "
          + "https://github.com/google/zetasql/issues/42")
  public void testQualifiedNameUdfQualifiedCallThrowsException() {
    String sql = "CREATE FUNCTION foo.bar.baz() AS (\"uwu\"); SELECT foo.bar.baz();";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("x").build()).addValue("uwu").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUnaryUdf() {
    String sql = "CREATE FUNCTION triple(x INT64) AS (3 * x); SELECT triple(triple(1));";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("x").build()).addValue(9L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUdfWithinUdf() {
    String sql =
        "CREATE FUNCTION triple(x INT64) AS (3 * x);"
            + " CREATE FUNCTION nonuple(x INT64) as (triple(triple(x)));"
            + " SELECT nonuple(1);";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("x").build()).addValue(9L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUndefinedUdfThrowsException() {
    String sql =
        "CREATE FUNCTION foo() AS (bar()); "
            + "CREATE FUNCTION bar() AS (foo()); "
            + "SELECT foo();";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(SqlException.class);
    thrown.expectMessage("Function not found: bar");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testRecursiveUdfThrowsException() {
    String sql = "CREATE FUNCTION omega() AS (omega()); SELECT omega();";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(SqlException.class);
    thrown.expectMessage("Function not found: omega");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testUDTVF() {
    String sql =
        "CREATE TABLE FUNCTION CustomerRange(MinID INT64, MaxID INT64)\n"
            + "  AS\n"
            + "    SELECT *\n"
            + "    FROM KeyValue\n"
            + "    WHERE key >= MinId AND key <= MaxId; \n"
            + " SELECT key FROM CustomerRange(10, 14)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField = Schema.builder().addInt64Field("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(singleField).addValues(14L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUDTVFTableNotFound() {
    String sql =
        "CREATE TABLE FUNCTION CustomerRange(MinID INT64, MaxID INT64)\n"
            + "  AS\n"
            + "    SELECT *\n"
            + "    FROM TableNotExist\n"
            + "    WHERE key >= MinId AND key <= MaxId; \n"
            + " SELECT key FROM CustomerRange(10, 14)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(SqlConversionException.class);
    thrown.expectMessage("Wasn't able to resolve the path [TableNotExist] in schema: beam");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testUDTVFFunctionNotFound() {
    String sql =
        "CREATE TABLE FUNCTION CustomerRange(MinID INT64, MaxID INT64)\n"
            + "  AS\n"
            + "    SELECT *\n"
            + "    FROM KeyValue\n"
            + "    WHERE key >= MinId AND key <= MaxId; \n"
            + " SELECT key FROM FunctionNotFound(10, 14)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(SqlException.class);
    thrown.expectMessage("Table-valued function not found: FunctionNotFound");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testDistinct() {
    String sql = "SELECT DISTINCT Key2 FROM aggregate_test_table";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema schema = Schema.builder().addInt64Field("Key2").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(10L).build(),
            Row.withSchema(schema).addValues(11L).build(),
            Row.withSchema(schema).addValues(12L).build(),
            Row.withSchema(schema).addValues(13L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDistinctOnNull() {
    String sql = "SELECT DISTINCT str_val FROM all_null_table";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema schema = Schema.builder().addNullableField("str_val", FieldType.DOUBLE).build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((Object) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testAnyValue() {
    String sql = "SELECT ANY_VALUE(double_val) FROM all_null_table";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema schema = Schema.builder().addNullableField("double_val", FieldType.DOUBLE).build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((Object) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectNULL() {
    String sql = "SELECT NULL";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema schema = Schema.builder().addNullableField("long_val", FieldType.INT64).build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((Object) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testWithQueryOne() {
    String sql =
        "With T1 AS (SELECT * FROM KeyValue), T2 AS (SELECT * FROM BigTable) SELECT T2.RowKey FROM"
            + " T1 INNER JOIN T2 on T1.Key = T2.RowKey;";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("field1").build())
                .addValues(15L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testWithQueryTwo() {
    String sql =
        "WITH T1 AS (SELECT Key, COUNT(*) as value FROM KeyValue GROUP BY Key) SELECT T1.Key,"
            + " T1.value FROM T1";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").addInt64Field("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(14L, 1L).build(),
            Row.withSchema(schema).addValues(15L, 1L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testWithQueryThree() {
    String sql =
        "WITH T1 as (SELECT Value, Key FROM KeyValue WHERE Key = 14 OR Key = 15) SELECT T1.Value,"
            + " T1.Key FROM T1;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").addInt64Field("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("KeyValue234", 14L).build(),
            Row.withSchema(schema).addValues("KeyValue235", 15L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testWithQueryFour() {
    String sql =
        "WITH T1 as (SELECT Value, Key FROM KeyValue) SELECT T1.Value, T1.Key FROM T1 WHERE T1.Key"
            + " = 14 OR T1.Key = 15;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field2").addInt64Field("field1").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("KeyValue234", 14L).build(),
            Row.withSchema(schema).addValues("KeyValue235", 15L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testWithQueryFive() {
    String sql =
        "WITH T1 AS (SELECT * FROM KeyValue) SELECT T1.Key, COUNT(*) FROM T1 GROUP BY T1.Key";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").addInt64Field("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(14L, 1L).build(),
            Row.withSchema(schema).addValues(15L, 1L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testWithQuerySix() {
    String sql =
        "WITH T1 AS (SELECT * FROM window_test_table_two) SELECT "
            + "COUNT(*) as field_count, "
            + "SESSION_START(\"INTERVAL 3 SECOND\") as window_start, "
            + "SESSION_END(\"INTERVAL 3 SECOND\") as window_end "
            + "FROM T1 "
            + "GROUP BY SESSION(ts, \"INTERVAL 3 SECOND\");";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("count_star")
            .addDateTimeField("field1")
            .addDateTimeField("field2")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    2L,
                    new DateTime(2018, 7, 1, 21, 26, 12, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 12, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    2L,
                    new DateTime(2018, 7, 1, 21, 26, 6, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 6, ISOChronology.getInstanceUTC()))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testWithQuerySeven() {
    String sql =
        "WITH t1 AS (select 1 AS k), t2 AS (select 1 AS k), t3 AS (select 1 AS k) "
            + "SELECT COUNT(*) "
            + "FROM t1 JOIN t3 USING (k)";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("count").build()).addValues(1L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testWithQueryEight() {
    String sql =
        "WITH T AS (SELECT k, 'hello' AS s FROM UNNEST([1, 2, 3]) k) "
            + "SELECT COUNT(*) "
            + "FROM T t1 JOIN T t2 USING (k)";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("count").build()).addValues(3L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUNNESTLiteral() {
    String sql = "SELECT * FROM UNNEST(ARRAY<STRING>['foo', 'bar']);";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema schema = Schema.builder().addStringField("str_field").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("foo").build(),
            Row.withSchema(schema).addValues("bar").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUNNESTParameters() {
    String sql = "SELECT * FROM UNNEST(@p0);";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createArrayValue(
                TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
                ImmutableList.of(Value.createStringValue("foo"), Value.createStringValue("bar"))));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema schema = Schema.builder().addStringField("str_field").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("foo").build(),
            Row.withSchema(schema).addValues("bar").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore("[BEAM-9515] ArrayScanToUncollectConverter Unnest does not support sub-queries")
  public void testUNNESTExpression() {
    String sql = "SELECT * FROM UNNEST(ARRAY(SELECT Value FROM KeyValue));";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema schema = Schema.builder().addStringField("str_field").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("KeyValue234").build(),
            Row.withSchema(schema).addValues("KeyValue235").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testNamedUNNESTLiteral() {
    String sql = "SELECT *, T1 FROM UNNEST(ARRAY<STRING>['foo', 'bar']) AS T1";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema schema =
        Schema.builder().addStringField("str_field").addStringField("str2_field").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("foo", "foo").build(),
            Row.withSchema(schema).addValues("bar", "bar").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testNamedUNNESTLiteralOffset() {
    String sql = "SELECT x, p FROM UNNEST([3, 4]) AS x WITH OFFSET p";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    thrown.expect(UnsupportedOperationException.class);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
  }

  @Test
  public void testUnnestArrayColumn() {
    String sql =
        "SELECT p FROM table_with_array_for_unnest, UNNEST(table_with_array_for_unnest.int_array_col) as p";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema schema = Schema.builder().addInt64Field("int_field").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValue(14L).build(),
            Row.withSchema(schema).addValue(18L).build(),
            Row.withSchema(schema).addValue(22L).build(),
            Row.withSchema(schema).addValue(24L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testStringAggregation() {
    String sql =
        "SELECT STRING_AGG(fruit) AS string_agg"
            + " FROM UNNEST([\"apple\", \"pear\", \"banana\", \"pear\"]) AS fruit";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema schema = Schema.builder().addStringField("string_field").build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValue("apple,pear,banana,pear").build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore("Seeing exception in Beam, need further investigation on the cause of this failed query.")
  public void testNamedUNNESTJoin() {
    String sql =
        "SELECT * "
            + "FROM table_with_array_for_unnest AS t1"
            + " LEFT JOIN UNNEST(t1.int_array_col) AS t2"
            + " on "
            + " t1.int_col = t2";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream).containsInAnyOrder();

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUnnestJoinStruct() {
    String sql =
        "SELECT b, x FROM UNNEST("
            + "[STRUCT(true AS b, [3, 5] AS arr), STRUCT(false AS b, [7, 9] AS arr)]) t "
            + "LEFT JOIN UNNEST(t.arr) x ON b";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(UnsupportedOperationException.class);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testUnnestJoinLiteral() {
    String sql =
        "SELECT a, b "
            + "FROM UNNEST([1, 1, 2, 3, 5, 8, 13, NULL]) a "
            + "JOIN UNNEST([1, 2, 3, 5, 7, 11, 13, NULL]) b "
            + "ON a = b";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(UnsupportedOperationException.class);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testUnnestJoinSubquery() {
    String sql =
        "SELECT a, b "
            + "FROM UNNEST([1, 2, 3]) a "
            + "JOIN UNNEST(ARRAY(SELECT b FROM UNNEST([3, 2, 1]) b)) b "
            + "ON a = b";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(UnsupportedOperationException.class);
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testCaseNoValue() {
    String sql = "SELECT CASE WHEN 1 > 2 THEN 'not possible' ELSE 'seems right' END";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("str_field").build())
                .addValue("seems right")
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCaseWithValue() {
    String sql = "SELECT CASE 1 WHEN 2 THEN 'not possible' ELSE 'seems right' END";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("str_field").build())
                .addValue("seems right")
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCaseWithValueMultipleCases() {
    String sql =
        "SELECT CASE 2 WHEN 1 THEN 'not possible' WHEN 2 THEN 'seems right' ELSE 'also not"
            + " possible' END";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("str_field").build())
                .addValue("seems right")
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCaseWithValueNoElse() {
    String sql = "SELECT CASE 2 WHEN 1 THEN 'not possible' WHEN 2 THEN 'seems right' END";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("str_field").build())
                .addValue("seems right")
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCaseNoValueNoElseNoMatch() {
    String sql = "SELECT CASE WHEN 'abc' = '123' THEN 'not possible' END";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addNullableField("str_field", FieldType.STRING).build())
                .addValue(null)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCaseWithValueNoElseNoMatch() {
    String sql = "SELECT CASE 2 WHEN 1 THEN 'not possible' END";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addNullableField("str_field", FieldType.STRING).build())
                .addValue(null)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCastToDateWithCase() {
    String sql =
        "SELECT f_int, \n"
            + "CASE WHEN CHAR_LENGTH(TRIM(f_string)) = 8 \n"
            + "    THEN CAST (CONCAT(\n"
            + "       SUBSTR(TRIM(f_string), 1, 4) \n"
            + "        , '-' \n"
            + "        , SUBSTR(TRIM(f_string), 5, 2) \n"
            + "        , '-' \n"
            + "        , SUBSTR(TRIM(f_string), 7, 2)) AS DATE)\n"
            + "    ELSE NULL\n"
            + "END \n"
            + "FROM table_for_case_when";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType =
        Schema.builder()
            .addInt64Field("f_long")
            .addNullableField("f_date", FieldType.logicalType(SqlTypes.DATE))
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(1L, LocalDate.parse("2018-10-18")).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIntersectAll() {
    String sql =
        "SELECT Key FROM aggregate_test_table "
            + "INTERSECT ALL "
            + "SELECT Key FROM aggregate_test_table_two";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType = Schema.builder().addInt64Field("field").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(1L).build(),
            Row.withSchema(resultType).addValues(2L).build(),
            Row.withSchema(resultType).addValues(2L).build(),
            Row.withSchema(resultType).addValues(2L).build(),
            Row.withSchema(resultType).addValues(3L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIntersectDistinct() {
    String sql =
        "SELECT Key FROM aggregate_test_table "
            + "INTERSECT DISTINCT "
            + "SELECT Key FROM aggregate_test_table_two";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType = Schema.builder().addInt64Field("field").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(1L).build(),
            Row.withSchema(resultType).addValues(2L).build(),
            Row.withSchema(resultType).addValues(3L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testExceptAll() {
    String sql =
        "SELECT Key FROM aggregate_test_table "
            + "EXCEPT ALL "
            + "SELECT Key FROM aggregate_test_table_two";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType = Schema.builder().addInt64Field("field").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(1L).build(),
            Row.withSchema(resultType).addValues(3L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectNullIntersectDistinct() {
    String sql = "SELECT NULL INTERSECT DISTINCT SELECT 2";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    System.err.println("SCHEMA " + stream.getSchema());

    PAssert.that(stream).empty();
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectNullIntersectAll() {
    String sql = "SELECT NULL INTERSECT ALL SELECT 2";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    System.err.println("SCHEMA " + stream.getSchema());

    PAssert.that(stream).empty();
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectNullExceptDistinct() {
    String sql = "SELECT NULL EXCEPT DISTINCT SELECT 2";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream).containsInAnyOrder(Row.nullRow(stream.getSchema()));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectNullExceptAll() {
    String sql = "SELECT NULL EXCEPT ALL SELECT 2";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream).containsInAnyOrder(Row.nullRow(stream.getSchema()));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectFromEmptyTable() {
    String sql = "SELECT * FROM table_empty;";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    PAssert.that(stream).containsInAnyOrder();
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testStartsWithString() {
    String sql = "SELECT STARTS_WITH('string1', 'stri')";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(true).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testStartsWithString2() {
    String sql = "SELECT STARTS_WITH(@p0, @p1)";

    ImmutableMap<String, Value> params =
        ImmutableMap.<String, Value>builder()
            .put("p0", Value.createSimpleNullValue(TypeKind.TYPE_STRING))
            .put("p1", Value.createStringValue(""))
            .build();

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((Boolean) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testStartsWithString3() {
    String sql = "SELECT STARTS_WITH(@p0, @p1)";

    ImmutableMap<String, Value> params =
        ImmutableMap.<String, Value>builder()
            .put("p0", Value.createSimpleNullValue(TypeKind.TYPE_STRING))
            .put("p1", Value.createSimpleNullValue(TypeKind.TYPE_STRING))
            .build();

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((Boolean) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEndsWithString() {
    String sql = "SELECT STARTS_WITH('string1', 'ng0')";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(false).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEndsWithString2() {
    String sql = "SELECT STARTS_WITH(@p0, @p1)";

    ImmutableMap<String, Value> params =
        ImmutableMap.<String, Value>builder()
            .put("p0", Value.createSimpleNullValue(TypeKind.TYPE_STRING))
            .put("p1", Value.createStringValue(""))
            .build();

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((Boolean) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEndsWithString3() {
    String sql = "SELECT STARTS_WITH(@p0, @p1)";

    ImmutableMap<String, Value> params =
        ImmutableMap.<String, Value>builder()
            .put("p0", Value.createSimpleNullValue(TypeKind.TYPE_STRING))
            .put("p1", Value.createSimpleNullValue(TypeKind.TYPE_STRING))
            .build();

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addNullableField("field1", FieldType.BOOLEAN).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((Boolean) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testConcatWithOneParameters() {
    String sql = "SELECT concat('abc')";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addStringField("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("abc").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testConcatWithTwoParameters() {
    String sql = "SELECT concat('abc', 'def')";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addStringField("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("abcdef").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testConcatWithThreeParameters() {
    String sql = "SELECT concat('abc', 'def', 'xyz')";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addStringField("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("abcdefxyz").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testConcatWithFourParameters() {
    String sql = "SELECT concat('abc', 'def', '  ', 'xyz')";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addStringField("field1").build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues("abcdef  xyz").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testConcatWithFiveParameters() {
    String sql = "SELECT concat('abc', 'def', '  ', 'xyz', 'kkk')";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addStringField("field1").build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues("abcdef  xyzkkk").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testConcatWithSixParameters() {
    String sql = "SELECT concat('abc', 'def', '  ', 'xyz', 'kkk', 'ttt')";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addStringField("field1").build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues("abcdef  xyzkkkttt").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testConcatWithNull1() {
    String sql = "SELECT CONCAT(@p0, @p1) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createStringValue(""),
            "p1",
            Value.createSimpleNullValue(TypeKind.TYPE_STRING));
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addNullableField("field1", FieldType.STRING).build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((String) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testConcatWithNull2() {
    String sql = "SELECT CONCAT(@p0, @p1) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
            Value.createSimpleNullValue(TypeKind.TYPE_STRING),
            "p1",
            Value.createSimpleNullValue(TypeKind.TYPE_STRING));
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addNullableField("field1", FieldType.STRING).build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((String) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testNamedParameterQuery() {
    String sql = "SELECT @ColA AS ColA";
    ImmutableMap<String, Value> params = ImmutableMap.of("ColA", Value.createInt64Value(5));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addInt64Field("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(5L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testArrayStructLiteral() {
    String sql = "SELECT ARRAY<STRUCT<INT64, INT64>>[(11, 12)];";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema innerSchema =
        Schema.of(Field.of("s", FieldType.INT64), Field.of("i", FieldType.INT64));
    final Schema schema =
        Schema.of(Field.of("field1", FieldType.array(FieldType.row(innerSchema))));

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValue(ImmutableList.of(Row.withSchema(innerSchema).addValues(11L, 12L).build()))
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testParameterStruct() {
    String sql = "SELECT @p as ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p",
            Value.createStructValue(
                TypeFactory.createStructType(
                    ImmutableList.of(
                        new StructType.StructField(
                            "s", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
                        new StructType.StructField(
                            "i", TypeFactory.createSimpleType(TypeKind.TYPE_INT64)))),
                ImmutableList.of(Value.createStringValue("foo"), Value.createInt64Value(1L))));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema innerSchema =
        Schema.of(Field.of("s", FieldType.STRING), Field.of("i", FieldType.INT64));
    final Schema schema = Schema.of(Field.of("field1", FieldType.row(innerSchema)));

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValue(Row.withSchema(innerSchema).addValues("foo", 1L).build())
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testParameterStructNested() {
    String sql = "SELECT @outer_struct.inner_struct.s as ColA";
    StructType innerStructType =
        TypeFactory.createStructType(
            ImmutableList.of(
                new StructType.StructField(
                    "s", TypeFactory.createSimpleType(TypeKind.TYPE_STRING))));
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "outer_struct",
            Value.createStructValue(
                TypeFactory.createStructType(
                    ImmutableList.of(new StructType.StructField("inner_struct", innerStructType))),
                ImmutableList.of(
                    Value.createStructValue(
                        innerStructType, ImmutableList.of(Value.createStringValue("foo"))))));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValue("foo").build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testConcatNamedParameterQuery() {
    String sql = "SELECT CONCAT(@p0, @p1) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of("p0", Value.createStringValue(""), "p1", Value.createStringValue("A"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addStringField("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("A").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testConcatPositionalParameterQuery() {
    String sql = "SELECT CONCAT(?, ?, ?) AS ColA";
    ImmutableList<Value> params =
        ImmutableList.of(
            Value.createStringValue("a"),
            Value.createStringValue("b"),
            Value.createStringValue("c"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addStringField("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("abc").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testReplace1() {
    String sql = "SELECT REPLACE(@p0, @p1, @p2) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue(""),
            "p1", Value.createStringValue(""),
            "p2", Value.createStringValue("a"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testReplace2() {
    String sql = "SELECT REPLACE(@p0, @p1, @p2) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue("abc"),
            "p1", Value.createStringValue(""),
            "p2", Value.createStringValue("xyz"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("abc").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testReplace3() {
    String sql = "SELECT REPLACE(@p0, @p1, @p2) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue(""),
            "p1", Value.createStringValue(""),
            "p2", Value.createSimpleNullValue(TypeKind.TYPE_STRING));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.STRING).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((String) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testReplace4() {
    String sql = "SELECT REPLACE(@p0, @p1, @p2) AS ColA";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createSimpleNullValue(TypeKind.TYPE_STRING),
            "p1", Value.createSimpleNullValue(TypeKind.TYPE_STRING),
            "p2", Value.createStringValue(""));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.STRING).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((String) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTrim1() {
    String sql = "SELECT trim(@p0)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of("p0", Value.createStringValue("   a b c   "));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("a b c").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTrim2() {
    String sql = "SELECT trim(@p0, @p1)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue("abxyzab"), "p1", Value.createStringValue("ab"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("xyz").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTrim3() {
    String sql = "SELECT trim(@p0, @p1)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createSimpleNullValue(TypeKind.TYPE_STRING),
            "p1", Value.createSimpleNullValue(TypeKind.TYPE_STRING));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.STRING).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((String) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLTrim1() {
    String sql = "SELECT ltrim(@p0)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of("p0", Value.createStringValue("   a b c   "));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("a b c   ").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLTrim2() {
    String sql = "SELECT ltrim(@p0, @p1)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue("abxyzab"), "p1", Value.createStringValue("ab"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("xyzab").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLTrim3() {
    String sql = "SELECT ltrim(@p0, @p1)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createSimpleNullValue(TypeKind.TYPE_STRING),
            "p1", Value.createSimpleNullValue(TypeKind.TYPE_STRING));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.STRING).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((String) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testRTrim1() {
    String sql = "SELECT rtrim(@p0)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of("p0", Value.createStringValue("   a b c   "));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("   a b c").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testRTrim2() {
    String sql = "SELECT rtrim(@p0, @p1)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue("abxyzab"), "p1", Value.createStringValue("ab"));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("abxyz").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testRTrim3() {
    String sql = "SELECT rtrim(@p0, @p1)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createSimpleNullValue(TypeKind.TYPE_STRING),
            "p1", Value.createSimpleNullValue(TypeKind.TYPE_STRING));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field1", FieldType.STRING).build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((String) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore("https://jira.apache.org/jira/browse/BEAM-9191")
  public void testCastBytesToString1() {
    String sql = "SELECT CAST(@p0 AS STRING)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of("p0", Value.createBytesValue(ByteString.copyFromUtf8("`")));
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("`").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCastBytesToString2() {
    String sql = "SELECT CAST(b'b' AS STRING)";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("b").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore("https://jira.apache.org/jira/browse/BEAM-9191")
  public void testCastBytesToStringFromTable() {
    String sql = "SELECT CAST(bytes_col AS STRING) FROM table_all_types";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("1").build(),
            Row.withSchema(schema).addValues("2").build(),
            Row.withSchema(schema).addValues("3").build(),
            Row.withSchema(schema).addValues("4").build(),
            Row.withSchema(schema).addValues("5").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCastStringToTimestamp() {
    String sql = "SELECT CAST('2019-01-15 13:21:03' AS TIMESTAMP)";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addDateTimeField("field_1").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(parseTimestampWithUTCTimeZone("2019-01-15 13:21:03"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  // test default timezone works properly in query analysis stage
  // TODO: Fix Later
  @SuppressWarnings("method.invocation.invalid")
  public void testCastStringToTimestampWithDefaultTimezoneSet() {
    String sql = "SELECT CAST('2014-12-01 12:34:56+07:30' AS TIMESTAMP)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    zetaSQLQueryPlanner.setDefaultTimezone("Pacific/Chatham");
    pipeline
        .getOptions()
        .as(BeamSqlPipelineOptions.class)
        .setZetaSqlDefaultTimezone("Pacific/Chatham");

    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDateTimeField("field_1").build())
                .addValues(parseTimestampWithUTCTimeZone("2014-12-01 05:04:56"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore("https://jira.apache.org/jira/browse/BEAM-10340")
  public void testCastBetweenTimeAndString() {
    String sql =
        "SELECT CAST(s1 as TIME) as t2, CAST(t1 as STRING) as s2 FROM "
            + "(SELECT '12:34:56.123456' as s1, TIME '12:34:56.123456' as t1)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addLogicalTypeField("t2", SqlTypes.TIME)
                        .addStringField("s2")
                        .build())
                .addValues(LocalTime.of(12, 34, 56, 123456000), "12:34:56.123456")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCastStringToString() {
    String sql = "SELECT CAST(@p0 AS STRING)";
    ImmutableMap<String, Value> params = ImmutableMap.of("p0", Value.createStringValue(""));
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCastStringToInt64() {
    String sql = "SELECT CAST(@p0 AS INT64)";
    ImmutableMap<String, Value> params = ImmutableMap.of("p0", Value.createStringValue("123"));
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(123L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectConstant() {
    String sql = "SELECT 'hi'";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("hi").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore("[BEAM-8593] ZetaSQL does not support Map type")
  public void testSelectFromTableWithMap() {
    String sql = "SELECT row_field FROM table_with_map";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema rowSchema = Schema.builder().addInt64Field("row_id").addStringField("data").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addRowField("row_field", rowSchema).build())
                .addValues(Row.withSchema(rowSchema).addValues(1L, "data1").build())
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSubQuery() {
    String sql = "select sum(Key) from KeyValue\n" + "group by (select Key)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Does not support sub-queries");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testSubstr() {
    String sql = "SELECT substr(@p0, @p1, @p2)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue("abc"),
            "p1", Value.createInt64Value(-2L),
            "p2", Value.createInt64Value(1L));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("b").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSubstrWithLargeValueExpectException() {
    String sql = "SELECT substr(@p0, @p1, @p2)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", Value.createStringValue("abc"),
            "p1", Value.createInt64Value(Integer.MAX_VALUE + 1L),
            "p2", Value.createInt64Value(Integer.MIN_VALUE - 1L));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    thrown.expect(RuntimeException.class);
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectAll() {
    String sql = "SELECT ALL Key, Value FROM KeyValue;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").addStringField("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(14L, "KeyValue234").build(),
            Row.withSchema(schema).addValues(15L, "KeyValue235").build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectDistinct() {
    String sql = "SELECT DISTINCT Key FROM aggregate_test_table;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(1L).build(),
            Row.withSchema(schema).addValues(2L).build(),
            Row.withSchema(schema).addValues(3L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectDistinct2() {
    String sql =
        "SELECT DISTINCT val.BYTES\n"
            + "from (select b\"BYTES\" BYTES union all\n"
            + "      select b\"bytes\" union all\n"
            + "      select b\"ByTeS\") val";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addByteArrayField("field1").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("BYTES".getBytes(StandardCharsets.UTF_8)).build(),
            Row.withSchema(schema).addValues("ByTeS".getBytes(StandardCharsets.UTF_8)).build(),
            Row.withSchema(schema).addValues("bytes".getBytes(StandardCharsets.UTF_8)).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectBytes() {
    String sql = "SELECT b\"ByTes\"";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addByteArrayField("field1").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("ByTes".getBytes(StandardCharsets.UTF_8)).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectExcept() {
    String sql = "SELECT * EXCEPT (Key, ts) FROM KeyValue;";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("KeyValue234").build(),
            Row.withSchema(schema).addValues("KeyValue235").build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSelectReplace() {
    String sql =
        "WITH orders AS\n"
            + "  (SELECT 5 as order_id,\n"
            + "  \"sprocket\" as item_name,\n"
            + "  200 as quantity)\n"
            + "SELECT * REPLACE (\"widget\" AS item_name)\n"
            + "FROM orders";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("field1")
            .addStringField("field2")
            .addInt64Field("field3")
            .build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues(5L, "widget", 200L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUnionAllBasic() {
    String sql =
        "SELECT row_id FROM table_all_types UNION ALL SELECT row_id FROM table_all_types_2";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValue(1L).build(),
            Row.withSchema(schema).addValue(2L).build(),
            Row.withSchema(schema).addValue(3L).build(),
            Row.withSchema(schema).addValue(4L).build(),
            Row.withSchema(schema).addValue(5L).build(),
            Row.withSchema(schema).addValue(6L).build(),
            Row.withSchema(schema).addValue(7L).build(),
            Row.withSchema(schema).addValue(8L).build(),
            Row.withSchema(schema).addValue(9L).build(),
            Row.withSchema(schema).addValue(10L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testAVGWithLongInput() {
    String sql = "SELECT AVG(f_int_1) FROM aggregate_test_table;";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(
        "AVG(LONG) is not supported. You might want to use AVG(CAST(expression AS DOUBLE).");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testReverseString() {
    String sql = "SELECT REVERSE('abc');";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addStringField("field2").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues("cba").build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCharLength() {
    String sql = "SELECT CHAR_LENGTH('abc');";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(3L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCharLengthNull() {
    String sql = "SELECT CHAR_LENGTH(@p0);";

    ImmutableMap<String, Value> params =
        ImmutableMap.of("p0", Value.createSimpleNullValue(TypeKind.TYPE_STRING));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addNullableField("field", FieldType.INT64).build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues((Object) null).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTumbleAsTVF() {
    String sql =
        "select Key, Value, ts, window_start, window_end from "
            + "TUMBLE((select * from KeyValue), descriptor(ts), 'INTERVAL 1 SECOND')";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    ImmutableMap<String, Value> params = ImmutableMap.of();
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("Key")
            .addStringField("Value")
            .addDateTimeField("ts")
            .addDateTimeField("window_start")
            .addDateTimeField("window_end")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    14L,
                    "KeyValue234",
                    DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:06"),
                    DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:06"),
                    DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:07"))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    15L,
                    "KeyValue235",
                    DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:07"),
                    DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:07"),
                    DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01T21:26:08"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIsNullTrueFalse() {
    String sql =
        "WITH Src AS (\n"
            + "  SELECT NULL as data UNION ALL\n"
            + "  SELECT TRUE UNION ALL\n"
            + "  SELECT FALSE\n"
            + ")\n"
            + "SELECT\n"
            + "  data IS NULL as isnull,\n"
            + "  data IS NOT NULL as isnotnull,\n"
            + "  data IS TRUE as istrue,\n"
            + "  data IS NOT TRUE as isnottrue,\n"
            + "  data IS FALSE as isfalse,\n"
            + "  data IS NOT FALSE as isnotfalse\n"
            + "FROM Src\n";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    ImmutableMap<String, Value> params = ImmutableMap.of();
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addField("isnull", FieldType.BOOLEAN)
            .addField("isnotnull", FieldType.BOOLEAN)
            .addField("istrue", FieldType.BOOLEAN)
            .addField("isnottrue", FieldType.BOOLEAN)
            .addField("isfalse", FieldType.BOOLEAN)
            .addField("isnotfalse", FieldType.BOOLEAN)
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(true, false, false, true, false, true).build(),
            Row.withSchema(schema).addValues(false, true, true, false, false, true).build(),
            Row.withSchema(schema).addValues(false, true, false, true, true, false).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLBitOr() {
    String sql = "SELECT BIT_OR(row_id) FROM table_all_types GROUP BY bool_col";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(3L).build(),
            Row.withSchema(schema).addValue(7L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore("NULL values don't work correctly. (https://issues.apache.org/jira/browse/BEAM-10379)")
  public void testZetaSQLBitAnd() {
    String sql = "SELECT BIT_AND(row_id) FROM table_all_types GROUP BY bool_col";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValue(1L).build(),
            Row.withSchema(schema).addValue(0L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSimpleTableName() {
    String sql = "SELECT Key FROM KeyValue";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField = Schema.builder().addInt64Field("field1").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(singleField).addValues(14L).build(),
            Row.withSchema(singleField).addValues(15L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }
}
