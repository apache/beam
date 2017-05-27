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
package org.apache.beam.dsls.sql.schema.transform;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.dsls.sql.planner.BeamQueryPlanner;
import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowCoder;
import org.apache.beam.dsls.sql.transform.BeamAggregationTransforms;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
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
 *
 */
public class BeamAggregationTransformTest extends BeamTransformBaseTest{

  @Rule
  public TestPipeline p = TestPipeline.create();

  private List<AggregateCall> aggCalls;

  private BeamSQLRecordType keyType;
  private BeamSQLRecordType aggPartType;
  private BeamSQLRecordType outputType;

  private BeamSqlRowCoder inRecordCoder;
  private BeamSqlRowCoder keyCoder;
  private BeamSqlRowCoder aggCoder;
  private BeamSqlRowCoder outRecordCoder;

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
   * @throws ParseException
   */
  @Test
  public void testCountPerElementBasic() throws ParseException {
    setupEnvironment();

    PCollection<BeamSQLRow> input = p.apply(Create.of(inputRows));

    //1. extract fields in group-by key part
    PCollection<KV<BeamSQLRow, BeamSQLRow>> exGroupByStream = input.apply("exGroupBy",
        WithKeys
            .of(new BeamAggregationTransforms.AggregationGroupByKeyFn(-1, ImmutableBitSet.of(0))))
        .setCoder(KvCoder.<BeamSQLRow, BeamSQLRow>of(keyCoder, inRecordCoder));

    //2. apply a GroupByKey.
    PCollection<KV<BeamSQLRow, Iterable<BeamSQLRow>>> groupedStream = exGroupByStream
        .apply("groupBy", GroupByKey.<BeamSQLRow, BeamSQLRow>create())
        .setCoder(KvCoder.<BeamSQLRow, Iterable<BeamSQLRow>>of(keyCoder,
            IterableCoder.<BeamSQLRow>of(inRecordCoder)));

    //3. run aggregation functions
    PCollection<KV<BeamSQLRow, BeamSQLRow>> aggregatedStream = groupedStream.apply("aggregation",
        Combine.<BeamSQLRow, BeamSQLRow, BeamSQLRow>groupedValues(
            new BeamAggregationTransforms.AggregationCombineFn(aggCalls, inputRowType)))
        .setCoder(KvCoder.<BeamSQLRow, BeamSQLRow>of(keyCoder, aggCoder));

    //4. flat KV to a single record
    PCollection<BeamSQLRow> mergedStream = aggregatedStream.apply("mergeRecord",
        ParDo.of(new BeamAggregationTransforms.MergeAggregationRecord(outputType, aggCalls)));
    mergedStream.setCoder(outRecordCoder);

    //assert function BeamAggregationTransform.AggregationGroupByKeyFn
    PAssert.that(exGroupByStream).containsInAnyOrder(prepareResultOfAggregationGroupByKeyFn());

    //assert BeamAggregationTransform.AggregationCombineFn
    PAssert.that(aggregatedStream).containsInAnyOrder(prepareResultOfAggregationCombineFn());

  //assert BeamAggregationTransform.MergeAggregationRecord
    PAssert.that(mergedStream).containsInAnyOrder(prepareResultOfMergeAggregationRecord());

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
        new AggregateCall(new SqlCountAggFunction(), false,
            Arrays.<Integer>asList(),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT),
            "count")
        );
    aggCalls.add(
        new AggregateCall(new SqlSumAggFunction(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT)), false,
            Arrays.<Integer>asList(1),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT),
            "sum1")
        );
    aggCalls.add(
        new AggregateCall(new SqlAvgAggFunction(SqlKind.AVG), false,
            Arrays.<Integer>asList(1),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT),
            "avg1")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MAX), false,
            Arrays.<Integer>asList(1),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT),
            "max1")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MIN), false,
            Arrays.<Integer>asList(1),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT),
            "min1")
        );

    aggCalls.add(
        new AggregateCall(new SqlSumAggFunction(
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.SMALLINT)), false,
            Arrays.<Integer>asList(2),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.SMALLINT),
            "sum2")
        );
    aggCalls.add(
        new AggregateCall(new SqlAvgAggFunction(SqlKind.AVG), false,
            Arrays.<Integer>asList(2),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.SMALLINT),
            "avg2")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MAX), false,
            Arrays.<Integer>asList(2),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.SMALLINT),
            "max2")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MIN), false,
            Arrays.<Integer>asList(2),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.SMALLINT),
            "min2")
        );

    aggCalls.add(
        new AggregateCall(
            new SqlSumAggFunction(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TINYINT)),
            false,
            Arrays.<Integer>asList(3),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TINYINT),
            "sum3")
        );
    aggCalls.add(
        new AggregateCall(new SqlAvgAggFunction(SqlKind.AVG), false,
            Arrays.<Integer>asList(3),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TINYINT),
            "avg3")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MAX), false,
            Arrays.<Integer>asList(3),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TINYINT),
            "max3")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MIN), false,
            Arrays.<Integer>asList(3),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TINYINT),
            "min3")
        );

    aggCalls.add(
        new AggregateCall(
            new SqlSumAggFunction(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT)),
            false,
            Arrays.<Integer>asList(4),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT),
            "sum4")
        );
    aggCalls.add(
        new AggregateCall(new SqlAvgAggFunction(SqlKind.AVG), false,
            Arrays.<Integer>asList(4),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT),
            "avg4")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MAX), false,
            Arrays.<Integer>asList(4),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT),
            "max4")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MIN), false,
            Arrays.<Integer>asList(4),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT),
            "min4")
        );

    aggCalls.add(
        new AggregateCall(
            new SqlSumAggFunction(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE)),
            false,
            Arrays.<Integer>asList(5),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE),
            "sum5")
        );
    aggCalls.add(
        new AggregateCall(new SqlAvgAggFunction(SqlKind.AVG), false,
            Arrays.<Integer>asList(5),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE),
            "avg5")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MAX), false,
            Arrays.<Integer>asList(5),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE),
            "max5")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MIN), false,
            Arrays.<Integer>asList(5),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE),
            "min5")
        );

    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MAX), false,
            Arrays.<Integer>asList(7),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIMESTAMP),
            "max7")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MIN), false,
            Arrays.<Integer>asList(7),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIMESTAMP),
            "min7")
        );

    aggCalls.add(
        new AggregateCall(
            new SqlSumAggFunction(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER)),
            false,
            Arrays.<Integer>asList(8),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER),
            "sum8")
        );
    aggCalls.add(
        new AggregateCall(new SqlAvgAggFunction(SqlKind.AVG), false,
            Arrays.<Integer>asList(8),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER),
            "avg8")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MAX), false,
            Arrays.<Integer>asList(8),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER),
            "max8")
        );
    aggCalls.add(
        new AggregateCall(new SqlMinMaxAggFunction(SqlKind.MIN), false,
            Arrays.<Integer>asList(8),
            new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER),
            "min8")
        );
  }

  /**
   * Coders used in aggregation steps.
   */
  private void prepareTypeAndCoder() {
    inRecordCoder = new BeamSqlRowCoder(inputRowType);

    keyType = initTypeOfSqlRow(Arrays.asList(KV.of("f_int", SqlTypeName.INTEGER)));
    keyCoder = new BeamSqlRowCoder(keyType);

    aggPartType = initTypeOfSqlRow(
        Arrays.asList(KV.of("count", SqlTypeName.BIGINT),

            KV.of("sum1", SqlTypeName.BIGINT), KV.of("avg1", SqlTypeName.BIGINT),
            KV.of("max1", SqlTypeName.BIGINT), KV.of("min1", SqlTypeName.BIGINT),

            KV.of("sum2", SqlTypeName.SMALLINT), KV.of("avg2", SqlTypeName.SMALLINT),
            KV.of("max2", SqlTypeName.SMALLINT), KV.of("min2", SqlTypeName.SMALLINT),

            KV.of("sum3", SqlTypeName.TINYINT), KV.of("avg3", SqlTypeName.TINYINT),
            KV.of("max3", SqlTypeName.TINYINT), KV.of("min3", SqlTypeName.TINYINT),

            KV.of("sum4", SqlTypeName.FLOAT), KV.of("avg4", SqlTypeName.FLOAT),
            KV.of("max4", SqlTypeName.FLOAT), KV.of("min4", SqlTypeName.FLOAT),

            KV.of("sum5", SqlTypeName.DOUBLE), KV.of("avg5", SqlTypeName.DOUBLE),
            KV.of("max5", SqlTypeName.DOUBLE), KV.of("min5", SqlTypeName.DOUBLE),

            KV.of("max7", SqlTypeName.TIMESTAMP), KV.of("min7", SqlTypeName.TIMESTAMP),

            KV.of("sum8", SqlTypeName.INTEGER), KV.of("avg8", SqlTypeName.INTEGER),
            KV.of("max8", SqlTypeName.INTEGER), KV.of("min8", SqlTypeName.INTEGER)
            ));
    aggCoder = new BeamSqlRowCoder(aggPartType);

    outputType = prepareFinalRowType();
    outRecordCoder = new BeamSqlRowCoder(outputType);
  }

  /**
   * expected results after {@link BeamAggregationTransforms.AggregationGroupByKeyFn}.
   */
  private List<KV<BeamSQLRow, BeamSQLRow>> prepareResultOfAggregationGroupByKeyFn() {
    return Arrays.asList(
        KV.of(new BeamSQLRow(keyType, Arrays.<Object>asList(inputRows.get(0).getInteger(0))),
            inputRows.get(0)),
        KV.of(new BeamSQLRow(keyType, Arrays.<Object>asList(inputRows.get(1).getInteger(0))),
            inputRows.get(1)),
        KV.of(new BeamSQLRow(keyType, Arrays.<Object>asList(inputRows.get(2).getInteger(0))),
            inputRows.get(2)),
        KV.of(new BeamSQLRow(keyType, Arrays.<Object>asList(inputRows.get(3).getInteger(0))),
            inputRows.get(3)));
  }

  /**
   * expected results after {@link BeamAggregationTransforms.AggregationCombineFn}.
   */
  private List<KV<BeamSQLRow, BeamSQLRow>> prepareResultOfAggregationCombineFn()
      throws ParseException {
    return Arrays.asList(
            KV.of(new BeamSQLRow(keyType, Arrays.<Object>asList(inputRows.get(0).getInteger(0))),
                new BeamSQLRow(aggPartType, Arrays.<Object>asList(
                    4L,
                    10000L, 2500L, 4000L, 1000L,
                    (short) 10, (short) 2, (short) 4, (short) 1,
                    (byte) 10, (byte) 2, (byte) 4, (byte) 1,
                    10.0F, 2.5F, 4.0F, 1.0F,
                    10.0, 2.5, 4.0, 1.0,
                    format.parse("2017-01-01 02:04:03"), format.parse("2017-01-01 01:01:03"),
                    10, 2, 4, 1
                    )))
            );
  }

  /**
   * Row type of final output row.
   */
  private BeamSQLRecordType prepareFinalRowType() {
    FieldInfoBuilder builder = BeamQueryPlanner.TYPE_FACTORY.builder();
    List<KV<String, SqlTypeName>> columnMetadata =
        Arrays.asList(KV.of("f_int", SqlTypeName.INTEGER), KV.of("count", SqlTypeName.BIGINT),

        KV.of("sum1", SqlTypeName.BIGINT), KV.of("avg1", SqlTypeName.BIGINT),
        KV.of("max1", SqlTypeName.BIGINT), KV.of("min1", SqlTypeName.BIGINT),

        KV.of("sum2", SqlTypeName.SMALLINT), KV.of("avg2", SqlTypeName.SMALLINT),
        KV.of("max2", SqlTypeName.SMALLINT), KV.of("min2", SqlTypeName.SMALLINT),

        KV.of("sum3", SqlTypeName.TINYINT), KV.of("avg3", SqlTypeName.TINYINT),
        KV.of("max3", SqlTypeName.TINYINT), KV.of("min3", SqlTypeName.TINYINT),

        KV.of("sum4", SqlTypeName.FLOAT), KV.of("avg4", SqlTypeName.FLOAT),
        KV.of("max4", SqlTypeName.FLOAT), KV.of("min4", SqlTypeName.FLOAT),

        KV.of("sum5", SqlTypeName.DOUBLE), KV.of("avg5", SqlTypeName.DOUBLE),
        KV.of("max5", SqlTypeName.DOUBLE), KV.of("min5", SqlTypeName.DOUBLE),

        KV.of("max7", SqlTypeName.TIMESTAMP), KV.of("min7", SqlTypeName.TIMESTAMP),

        KV.of("sum8", SqlTypeName.INTEGER), KV.of("avg8", SqlTypeName.INTEGER),
        KV.of("max8", SqlTypeName.INTEGER), KV.of("min8", SqlTypeName.INTEGER)
        );
    for (KV<String, SqlTypeName> cm : columnMetadata) {
      builder.add(cm.getKey(), cm.getValue());
    }
    return BeamSQLRecordType.from(builder.build());
  }

  /**
   * expected results after {@link BeamAggregationTransforms.MergeAggregationRecord}.
   */
  private BeamSQLRow prepareResultOfMergeAggregationRecord() throws ParseException {
    return new BeamSQLRow(outputType, Arrays.<Object>asList(
        1, 4L,
        10000L, 2500L, 4000L, 1000L,
        (short) 10, (short) 2, (short) 4, (short) 1,
        (byte) 10, (byte) 2, (byte) 4, (byte) 1,
        10.0F, 2.5F, 4.0F, 1.0F,
        10.0, 2.5, 4.0, 1.0,
        format.parse("2017-01-01 02:04:03"), format.parse("2017-01-01 01:01:03"),
        10, 2, 4, 1
        ));
  }
}
