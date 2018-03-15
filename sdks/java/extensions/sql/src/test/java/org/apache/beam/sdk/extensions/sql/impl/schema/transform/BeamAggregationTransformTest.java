/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.impl.schema.transform;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamAggregationTransforms;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link BeamAggregationTransforms}.
 */
public class BeamAggregationTransformTest extends BeamTransformBaseTest {

  @Rule
  public TestPipeline p = TestPipeline.create();

  private List<AggregateCall> aggCalls;

  private Schema keyType;
  private Schema aggPartType;
  private Schema outputType;

  private RowCoder inRecordCoder;
  private RowCoder keyCoder;
  private RowCoder aggCoder;
  private RowCoder outRecordCoder;

  /**
   * This step equals to below query.
   * <pre>
   * SELECT `f_int`
   * , COUNT(*) AS `size`
   * , SUM(`f_long`) AS `sum1`, AVG(`f_long`) AS `avg1`
   * , MAX(`f_long`) AS `max1`, MIN(`f_long`) AS `min1`
   * , SUM(`f_short`) AS `sum2`, AVG(`f_short`) AS `avg2`
   * , MAX(`f_short`) AS `max2`, MIN(`f_short`) AS `min2`
   * , SUM(`f_byte`) AS `sum3`, AVG(`f_byte`) AS `avg3`
   * , MAX(`f_byte`) AS `max3`, MIN(`f_byte`) AS `min3`
   * , SUM(`f_float`) AS `sum4`, AVG(`f_float`) AS `avg4`
   * , MAX(`f_float`) AS `max4`, MIN(`f_float`) AS `min4`
   * , SUM(`f_double`) AS `sum5`, AVG(`f_double`) AS `avg5`
   * , MAX(`f_double`) AS `max5`, MIN(`f_double`) AS `min5`
   * , MAX(`f_timestamp`) AS `max7`, MIN(`f_timestamp`) AS `min7`
   * ,SUM(`f_int2`) AS `sum8`, AVG(`f_int2`) AS `avg8`
   * , MAX(`f_int2`) AS `max8`, MIN(`f_int2`) AS `min8`
   * FROM TABLE_NAME
   * GROUP BY `f_int`
   * </pre>
   *
   * @throws ParseException
   */
  @Test
  public void testCountPerElementBasic() throws ParseException {
    setupEnvironment();

    PCollection<Row> input = p.apply(Create.of(inputRows));

    // 1. extract fields in group-by key part
    PCollection<KV<Row, Row>> exGroupByStream =
        input
            .apply(
                "exGroupBy",
                WithKeys.of(
                    new BeamAggregationTransforms.AggregationGroupByKeyFn(
                        -1, ImmutableBitSet.of(0))))
            .setCoder(KvCoder.of(keyCoder, inRecordCoder));

    // 2. apply a GroupByKey.
    PCollection<KV<Row, Iterable<Row>>> groupedStream =
        exGroupByStream
            .apply("groupBy", GroupByKey.create())
            .setCoder(KvCoder.of(keyCoder, IterableCoder.of(inRecordCoder)));

    // 3. run aggregation functions
    PCollection<KV<Row, Row>> aggregatedStream =
        groupedStream
            .apply(
                "aggregation",
                Combine.groupedValues(
                    new BeamAggregationTransforms.AggregationAdaptor(aggCalls, inputSchema)))
            .setCoder(KvCoder.of(keyCoder, aggCoder));

    //4. flat KV to a single record
    PCollection<Row> mergedStream = aggregatedStream.apply(
        "mergeRecord",
        ParDo.of(new BeamAggregationTransforms.MergeAggregationRecord(outputType, aggCalls, -1)));
    mergedStream.setCoder(outRecordCoder);

    //assert function BeamAggregationTransform.AggregationGroupByKeyFn
    PAssert.that(exGroupByStream).containsInAnyOrder(prepareResultOfAggregationGroupByKeyFn());

    //assert BeamAggregationTransform.AggregationCombineFn
    PAssert.that(aggregatedStream).containsInAnyOrder(prepareResultOfAggregationCombineFn());

    //assert BeamAggregationTransform.MergeAggregationRecord
    PAssert.that(mergedStream).containsInAnyOrder(prepareResultOfMergeAggregationRow());

    p.run();
  }

  private void setupEnvironment() {
    prepareAggregationCalls();
    prepareTypeAndCoder();
  }

  /**
   * create list of all {@link AggregateCall}.
   */
  @SuppressWarnings("deprecation")
  private void prepareAggregationCalls() {
    //aggregations for all data type
    aggCalls = new ArrayList<>();
    aggCalls.add(
        new AggregateCall(
            new SqlCountAggFunction("COUNT"),
            false,
            Arrays.asList(),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT),
            "count"));
    aggCalls.add(
        new AggregateCall(
            new SqlSumAggFunction(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT)),
            false,
            Arrays.asList(1),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT),
            "sum1"));
    aggCalls.add(
        new AggregateCall(
            new SqlAvgAggFunction(SqlKind.AVG),
            false,
            Arrays.asList(1),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT),
            "avg1"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MAX),
            false,
            Arrays.asList(1),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT),
            "max1"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MIN),
            false,
            Arrays.asList(1),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT),
            "min1"));

    aggCalls.add(
        new AggregateCall(
            new SqlSumAggFunction(
                new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.SMALLINT)),
            false,
            Arrays.asList(2),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.SMALLINT),
            "sum2"));
    aggCalls.add(
        new AggregateCall(
            new SqlAvgAggFunction(SqlKind.AVG),
            false,
            Arrays.asList(2),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.SMALLINT),
            "avg2"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MAX),
            false,
            Arrays.asList(2),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.SMALLINT),
            "max2"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MIN),
            false,
            Arrays.asList(2),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.SMALLINT),
            "min2"));

    aggCalls.add(
        new AggregateCall(
            new SqlSumAggFunction(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TINYINT)),
            false,
            Arrays.asList(3),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TINYINT),
            "sum3"));
    aggCalls.add(
        new AggregateCall(
            new SqlAvgAggFunction(SqlKind.AVG),
            false,
            Arrays.asList(3),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TINYINT),
            "avg3"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MAX),
            false,
            Arrays.asList(3),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TINYINT),
            "max3"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MIN),
            false,
            Arrays.asList(3),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TINYINT),
            "min3"));

    aggCalls.add(
        new AggregateCall(
            new SqlSumAggFunction(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT)),
            false,
            Arrays.asList(4),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT),
            "sum4"));
    aggCalls.add(
        new AggregateCall(
            new SqlAvgAggFunction(SqlKind.AVG),
            false,
            Arrays.asList(4),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT),
            "avg4"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MAX),
            false,
            Arrays.asList(4),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT),
            "max4"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MIN),
            false,
            Arrays.asList(4),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT),
            "min4"));

    aggCalls.add(
        new AggregateCall(
            new SqlSumAggFunction(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE)),
            false,
            Arrays.asList(5),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE),
            "sum5"));
    aggCalls.add(
        new AggregateCall(
            new SqlAvgAggFunction(SqlKind.AVG),
            false,
            Arrays.asList(5),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE),
            "avg5"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MAX),
            false,
            Arrays.asList(5),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE),
            "max5"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MIN),
            false,
            Arrays.asList(5),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE),
            "min5"));

    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MAX),
            false,
            Arrays.asList(7),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIMESTAMP),
            "max7"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MIN),
            false,
            Arrays.asList(7),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIMESTAMP),
            "min7"));

    aggCalls.add(
        new AggregateCall(
            new SqlSumAggFunction(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER)),
            false,
            Arrays.asList(8),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER),
            "sum8"));
    aggCalls.add(
        new AggregateCall(
            new SqlAvgAggFunction(SqlKind.AVG),
            false,
            Arrays.asList(8),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER),
            "avg8"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MAX),
            false,
            Arrays.asList(8),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER),
            "max8"));
    aggCalls.add(
        new AggregateCall(
            new SqlMinMaxAggFunction(SqlKind.MIN),
            false,
            Arrays.asList(8),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER),
            "min8"));
  }

  /**
   * Coders used in aggregation steps.
   */
  private void prepareTypeAndCoder() {
    inRecordCoder = inputSchema.getRowCoder();

    keyType =
        RowSqlType
            .builder()
            .withIntegerField("f_int")
            .build();

    keyCoder = keyType.getRowCoder();

    aggPartType = RowSqlType
        .builder()
        .withBigIntField("count")

        .withBigIntField("sum1")
        .withBigIntField("avg1")
        .withBigIntField("max1")
        .withBigIntField("min1")

        .withSmallIntField("sum2")
        .withSmallIntField("avg2")
        .withSmallIntField("max2")
        .withSmallIntField("min2")

        .withTinyIntField("sum3")
        .withTinyIntField("avg3")
        .withTinyIntField("max3")
        .withTinyIntField("min3")

        .withFloatField("sum4")
        .withFloatField("avg4")
        .withFloatField("max4")
        .withFloatField("min4")

        .withDoubleField("sum5")
        .withDoubleField("avg5")
        .withDoubleField("max5")
        .withDoubleField("min5")

        .withTimestampField("max7")
        .withTimestampField("min7")

        .withIntegerField("sum8")
        .withIntegerField("avg8")
        .withIntegerField("max8")
        .withIntegerField("min8")

        .build();

    aggCoder = aggPartType.getRowCoder();

    outputType = prepareFinalRowType();
    outRecordCoder = outputType.getRowCoder();
  }

  /**
   * expected results after {@link BeamAggregationTransforms.AggregationGroupByKeyFn}.
   */
  private List<KV<Row, Row>> prepareResultOfAggregationGroupByKeyFn() {
    return
        IntStream
            .range(0, 4)
            .mapToObj(i -> KV.of(
                Row
                    .withRowType(keyType)
                    .addValues(inputRows.get(i).getInteger(0))
                    .build(),
                inputRows.get(i)
            )).collect(Collectors.toList());
  }

  /**
   * expected results.
   */
  private List<KV<Row, Row>> prepareResultOfAggregationCombineFn()
      throws ParseException {
    return Arrays.asList(
        KV.of(
            Row
                .withRowType(keyType)
                .addValues(inputRows.get(0).getInteger(0))
                .build(),
            Row
                .withRowType(aggPartType)
                .addValues(
                    4L,
                    10000L,
                    2500L,
                    4000L,
                    1000L,
                    (short) 10,
                    (short) 2,
                    (short) 4,
                    (short) 1,
                    (byte) 10,
                    (byte) 2,
                    (byte) 4,
                    (byte) 1,
                    10.0F,
                    2.5F,
                    4.0F,
                    1.0F,
                    10.0,
                    2.5,
                    4.0,
                    1.0,
                    format.parse("2017-01-01 02:04:03"),
                    format.parse("2017-01-01 01:01:03"),
                    10,
                    2,
                    4,
                    1)
                .build()));
  }


  /**
   * Row type of final output row.
   */
  private Schema prepareFinalRowType() {
    return
        RowSqlType
            .builder()
            .withIntegerField("f_int")
            .withBigIntField("count")

            .withBigIntField("sum1")
            .withBigIntField("avg1")
            .withBigIntField("max1")
            .withBigIntField("min1")

            .withSmallIntField("sum2")
            .withSmallIntField("avg2")
            .withSmallIntField("max2")
            .withSmallIntField("min2")

            .withTinyIntField("sum3")
            .withTinyIntField("avg3")
            .withTinyIntField("max3")
            .withTinyIntField("min3")

            .withFloatField("sum4")
            .withFloatField("avg4")
            .withFloatField("max4")
            .withFloatField("min4")

            .withDoubleField("sum5")
            .withDoubleField("avg5")
            .withDoubleField("max5")
            .withDoubleField("min5")

            .withTimestampField("max7")
            .withTimestampField("min7")

            .withIntegerField("sum8")
            .withIntegerField("avg8")
            .withIntegerField("max8")
            .withIntegerField("min8")

            .build();
  }

  /**
   * expected results after {@link BeamAggregationTransforms.MergeAggregationRecord}.
   */
  private Row prepareResultOfMergeAggregationRow() throws ParseException {
    return Row
        .withRowType(outputType)
        .addValues(
            1,
            4L,
            10000L,
            2500L,
            4000L,
            1000L,
            (short) 10,
            (short) 2,
            (short) 4,
            (short) 1,
            (byte) 10,
            (byte) 2,
            (byte) 4,
            (byte) 1,
            10.0F,
            2.5F,
            4.0F,
            1.0F,
            10.0,
            2.5,
            4.0,
            1.0,
            format.parse("2017-01-01 02:04:03"),
            format.parse("2017-01-01 01:01:03"),
            10,
            2,
            4,
            1)
        .build();
  }
}
