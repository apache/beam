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
package org.apache.beam.dsls.sql.transform;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.beam.dsls.sql.exception.BeamSqlUnsupportedException;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.joda.time.Instant;

/**
 * Collections of {@code PTransform} and {@code DoFn} used to perform GROUP-BY operation.
 */
public class BeamAggregationTransforms implements Serializable{
  /**
   * Merge KV to single record.
   */
  public static class MergeAggregationRecord extends DoFn<KV<BeamSqlRow, BeamSqlRow>, BeamSqlRow> {
    private BeamSqlRecordType outRecordType;
    private List<String> aggFieldNames;

    public MergeAggregationRecord(BeamSqlRecordType outRecordType, List<AggregateCall> aggList) {
      this.outRecordType = outRecordType;
      this.aggFieldNames = new ArrayList<>();
      for (AggregateCall ac : aggList) {
        aggFieldNames.add(ac.getName());
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      BeamSqlRow outRecord = new BeamSqlRow(outRecordType);
      outRecord.updateWindowRange(c.element().getKey(), window);

      KV<BeamSqlRow, BeamSqlRow> kvRecord = c.element();
      for (String f : kvRecord.getKey().getDataType().getFieldsName()) {
        outRecord.addField(f, kvRecord.getKey().getFieldValue(f));
      }
      for (int idx = 0; idx < aggFieldNames.size(); ++idx) {
        outRecord.addField(aggFieldNames.get(idx), kvRecord.getValue().getFieldValue(idx));
      }

      // if (c.pane().isLast()) {
      c.output(outRecord);
      // }
    }
  }

  /**
   * extract group-by fields.
   */
  public static class AggregationGroupByKeyFn
      implements SerializableFunction<BeamSqlRow, BeamSqlRow> {
    private List<Integer> groupByKeys;

    public AggregationGroupByKeyFn(int windowFieldIdx, ImmutableBitSet groupSet) {
      this.groupByKeys = new ArrayList<>();
      for (int i : groupSet.asList()) {
        if (i != windowFieldIdx) {
          groupByKeys.add(i);
        }
      }
    }

    @Override
    public BeamSqlRow apply(BeamSqlRow input) {
      BeamSqlRecordType typeOfKey = exTypeOfKeyRecord(input.getDataType());
      BeamSqlRow keyOfRecord = new BeamSqlRow(typeOfKey);
      keyOfRecord.updateWindowRange(input, null);

      for (int idx = 0; idx < groupByKeys.size(); ++idx) {
        keyOfRecord.addField(idx, input.getFieldValue(groupByKeys.get(idx)));
      }
      return keyOfRecord;
    }

    private BeamSqlRecordType exTypeOfKeyRecord(BeamSqlRecordType dataType) {
      BeamSqlRecordType typeOfKey = new BeamSqlRecordType();
      for (int idx : groupByKeys) {
        typeOfKey.addField(dataType.getFieldsName().get(idx), dataType.getFieldsType().get(idx));
      }
      return typeOfKey;
    }

  }

  /**
   * Assign event timestamp.
   */
  public static class WindowTimestampFn implements SerializableFunction<BeamSqlRow, Instant> {
    private int windowFieldIdx = -1;

    public WindowTimestampFn(int windowFieldIdx) {
      super();
      this.windowFieldIdx = windowFieldIdx;
    }

    @Override
    public Instant apply(BeamSqlRow input) {
      return new Instant(input.getDate(windowFieldIdx).getTime());
    }
  }

  /**
   * Aggregation function which supports COUNT, MAX, MIN, SUM, AVG.
   *
   * <p>Multiple aggregation functions are combined together.
   * For each aggregation function, it may accept part of all data types:<br>
   *   1). COUNT works for any data type;<br>
   *   2). MAX/MIN works for INT, LONG, FLOAT, DOUBLE, DECIMAL, SMALLINT, TINYINT, TIMESTAMP;<br>
   *   3). SUM/AVG works for INT, LONG, FLOAT, DOUBLE, DECIMAL, SMALLINT, TINYINT;<br>
   *
   */
  public static class AggregationCombineFn extends CombineFn<BeamSqlRow, BeamSqlRow, BeamSqlRow> {
    private BeamSqlRecordType aggDataType;

    private int countIndex = -1;

    List<String> aggFunctions;
    List<BeamSqlExpression> aggElementExpressions;

    public AggregationCombineFn(List<AggregateCall> aggregationCalls,
        BeamSqlRecordType sourceRowRecordType) {
      this.aggDataType = new BeamSqlRecordType();
      this.aggFunctions = new ArrayList<>();
      this.aggElementExpressions = new ArrayList<>();

      boolean hasAvg = false;
      boolean hasCount = false;
      int countIndex = -1;
      for (int idx = 0; idx < aggregationCalls.size(); ++idx) {
        AggregateCall ac = aggregationCalls.get(idx);
        //verify it's supported.
        verifySupportedAggregation(ac);

        aggDataType.addField(ac.name, ac.type.getSqlTypeName());

        SqlAggFunction aggFn = ac.getAggregation();
        switch (aggFn.getName()) {
        case "COUNT":
          aggElementExpressions.add(BeamSqlPrimitive.<Long>of(SqlTypeName.BIGINT, 1L));
          hasCount = true;
          countIndex = idx;
          break;
        case "SUM":
        case "MAX":
        case "MIN":
        case "AVG":
          int refIndex = ac.getArgList().get(0);
          aggElementExpressions.add(new BeamSqlInputRefExpression(
              sourceRowRecordType.getFieldsType().get(refIndex), refIndex));
          if ("AVG".equals(aggFn.getName())) {
            hasAvg = true;
          }
          break;

        default:
          break;
        }
        aggFunctions.add(aggFn.getName());
      }
      // add a COUNT holder if only have AVG
      if (hasAvg && !hasCount) {
        aggDataType.addField("__COUNT", SqlTypeName.BIGINT);

        aggFunctions.add("COUNT");
        aggElementExpressions.add(BeamSqlPrimitive.<Long>of(SqlTypeName.BIGINT, 1L));

        hasCount = true;
        countIndex = aggDataType.size() - 1;
      }

      this.countIndex = countIndex;
    }

    private void verifySupportedAggregation(AggregateCall ac) {
      //donot support DISTINCT
      if (ac.isDistinct()) {
        throw new BeamSqlUnsupportedException("DISTINCT is not supported yet.");
      }
      String aggFnName = ac.getAggregation().getName();
      switch (aggFnName) {
      case "COUNT":
        //COUNT works for any data type;
        break;
      case "SUM":
        // SUM only support for INT, LONG, FLOAT, DOUBLE, DECIMAL, SMALLINT,
        // TINYINT now
        if (!Arrays
            .asList(SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.FLOAT, SqlTypeName.DOUBLE,
                SqlTypeName.SMALLINT, SqlTypeName.TINYINT)
            .contains(ac.type.getSqlTypeName())) {
          throw new BeamSqlUnsupportedException(
              "SUM only support for INT, LONG, FLOAT, DOUBLE, SMALLINT, TINYINT");
        }
        break;
      case "MAX":
      case "MIN":
        // MAX/MIN only support for INT, LONG, FLOAT, DOUBLE, DECIMAL, SMALLINT,
        // TINYINT, TIMESTAMP now
        if (!Arrays.asList(SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.FLOAT,
            SqlTypeName.DOUBLE, SqlTypeName.SMALLINT, SqlTypeName.TINYINT,
            SqlTypeName.TIMESTAMP).contains(ac.type.getSqlTypeName())) {
          throw new BeamSqlUnsupportedException("MAX/MIN only support for INT, LONG, FLOAT,"
              + " DOUBLE, SMALLINT, TINYINT, TIMESTAMP");
        }
        break;
      case "AVG":
        // AVG only support for INT, LONG, FLOAT, DOUBLE, DECIMAL, SMALLINT,
        // TINYINT now
        if (!Arrays
            .asList(SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.FLOAT, SqlTypeName.DOUBLE,
                SqlTypeName.SMALLINT, SqlTypeName.TINYINT)
            .contains(ac.type.getSqlTypeName())) {
          throw new BeamSqlUnsupportedException(
              "AVG only support for INT, LONG, FLOAT, DOUBLE, SMALLINT, TINYINT");
        }
        break;
      default:
        throw new BeamSqlUnsupportedException(
            String.format("[%s] is not supported.", aggFnName));
      }
    }

    @Override
    public BeamSqlRow createAccumulator() {
      BeamSqlRow initialRecord = new BeamSqlRow(aggDataType);
      for (int idx = 0; idx < aggElementExpressions.size(); ++idx) {
        BeamSqlExpression ex = aggElementExpressions.get(idx);
        String aggFnName = aggFunctions.get(idx);
        switch (aggFnName) {
        case "COUNT":
          initialRecord.addField(idx, 0L);
          break;
        case "AVG":
        case "SUM":
          //for both AVG/SUM, a summary value is hold at first.
          switch (ex.getOutputType()) {
          case INTEGER:
            initialRecord.addField(idx, 0);
            break;
          case BIGINT:
            initialRecord.addField(idx, 0L);
            break;
          case SMALLINT:
            initialRecord.addField(idx, (short) 0);
            break;
          case TINYINT:
            initialRecord.addField(idx, (byte) 0);
            break;
          case FLOAT:
            initialRecord.addField(idx, 0.0f);
            break;
          case DOUBLE:
            initialRecord.addField(idx, 0.0);
            break;
          default:
            break;
          }
          break;
        case "MAX":
          switch (ex.getOutputType()) {
          case INTEGER:
            initialRecord.addField(idx, Integer.MIN_VALUE);
            break;
          case BIGINT:
            initialRecord.addField(idx, Long.MIN_VALUE);
            break;
          case SMALLINT:
            initialRecord.addField(idx, Short.MIN_VALUE);
            break;
          case TINYINT:
            initialRecord.addField(idx, Byte.MIN_VALUE);
            break;
          case FLOAT:
            initialRecord.addField(idx, Float.MIN_VALUE);
            break;
          case DOUBLE:
            initialRecord.addField(idx, Double.MIN_VALUE);
            break;
          case TIMESTAMP:
            initialRecord.addField(idx, new Date(0));
            break;
          default:
            break;
          }
          break;
        case "MIN":
          switch (ex.getOutputType()) {
          case INTEGER:
            initialRecord.addField(idx, Integer.MAX_VALUE);
            break;
          case BIGINT:
            initialRecord.addField(idx, Long.MAX_VALUE);
            break;
          case SMALLINT:
            initialRecord.addField(idx, Short.MAX_VALUE);
            break;
          case TINYINT:
            initialRecord.addField(idx, Byte.MAX_VALUE);
            break;
          case FLOAT:
            initialRecord.addField(idx, Float.MAX_VALUE);
            break;
          case DOUBLE:
            initialRecord.addField(idx, Double.MAX_VALUE);
            break;
          case TIMESTAMP:
            initialRecord.addField(idx, new Date(Long.MAX_VALUE));
            break;
          default:
            break;
          }
          break;
        default:
          break;
        }
      }
      return initialRecord;
    }

    @Override
    public BeamSqlRow addInput(BeamSqlRow accumulator, BeamSqlRow input) {
      BeamSqlRow deltaRecord = new BeamSqlRow(aggDataType);
      for (int idx = 0; idx < aggElementExpressions.size(); ++idx) {
        BeamSqlExpression ex = aggElementExpressions.get(idx);
        String aggFnName = aggFunctions.get(idx);
        switch (aggFnName) {
        case "COUNT":
          deltaRecord.addField(idx, 1 + accumulator.getLong(idx));
          break;
        case "AVG":
        case "SUM":
          // for both AVG/SUM, a summary value is hold at first.
          switch (ex.getOutputType()) {
          case INTEGER:
            deltaRecord.addField(idx,
                ex.evaluate(input).getInteger() + accumulator.getInteger(idx));
            break;
          case BIGINT:
            deltaRecord.addField(idx, ex.evaluate(input).getLong() + accumulator.getLong(idx));
            break;
          case SMALLINT:
            deltaRecord.addField(idx,
                (short) (ex.evaluate(input).getShort() + accumulator.getShort(idx)));
            break;
          case TINYINT:
            deltaRecord.addField(idx,
                (byte) (ex.evaluate(input).getByte() + accumulator.getByte(idx)));
            break;
          case FLOAT:
            deltaRecord.addField(idx,
                (float) (ex.evaluate(input).getFloat() + accumulator.getFloat(idx)));
            break;
          case DOUBLE:
            deltaRecord.addField(idx, ex.evaluate(input).getDouble() + accumulator.getDouble(idx));
            break;
          default:
            break;
          }
          break;
        case "MAX":
          switch (ex.getOutputType()) {
          case INTEGER:
            deltaRecord.addField(idx,
                Math.max(ex.evaluate(input).getInteger(), accumulator.getInteger(idx)));
            break;
          case BIGINT:
            deltaRecord.addField(idx,
                Math.max(ex.evaluate(input).getLong(), accumulator.getLong(idx)));
            break;
          case SMALLINT:
            deltaRecord.addField(idx,
                (short) Math.max(ex.evaluate(input).getShort(), accumulator.getShort(idx)));
            break;
          case TINYINT:
            deltaRecord.addField(idx,
                (byte) Math.max(ex.evaluate(input).getByte(), accumulator.getByte(idx)));
            break;
          case FLOAT:
            deltaRecord.addField(idx,
                Math.max(ex.evaluate(input).getFloat(), accumulator.getFloat(idx)));
            break;
          case DOUBLE:
            deltaRecord.addField(idx,
                Math.max(ex.evaluate(input).getDouble(), accumulator.getDouble(idx)));
            break;
          case TIMESTAMP:
            Date preDate = accumulator.getDate(idx);
            Date nowDate = ex.evaluate(input).getDate();
            deltaRecord.addField(idx, preDate.getTime() > nowDate.getTime() ? preDate : nowDate);
            break;
          default:
            break;
          }
          break;
        case "MIN":
          switch (ex.getOutputType()) {
          case INTEGER:
            deltaRecord.addField(idx,
                Math.min(ex.evaluate(input).getInteger(), accumulator.getInteger(idx)));
            break;
          case BIGINT:
            deltaRecord.addField(idx,
                Math.min(ex.evaluate(input).getLong(), accumulator.getLong(idx)));
            break;
          case SMALLINT:
            deltaRecord.addField(idx,
                (short) Math.min(ex.evaluate(input).getShort(), accumulator.getShort(idx)));
            break;
          case TINYINT:
            deltaRecord.addField(idx,
                (byte) Math.min(ex.evaluate(input).getByte(), accumulator.getByte(idx)));
            break;
          case FLOAT:
            deltaRecord.addField(idx,
                Math.min(ex.evaluate(input).getFloat(), accumulator.getFloat(idx)));
            break;
          case DOUBLE:
            deltaRecord.addField(idx,
                Math.min(ex.evaluate(input).getDouble(), accumulator.getDouble(idx)));
            break;
          case TIMESTAMP:
            Date preDate = accumulator.getDate(idx);
            Date nowDate = ex.evaluate(input).getDate();
            deltaRecord.addField(idx, preDate.getTime() < nowDate.getTime() ? preDate : nowDate);
            break;
          default:
            break;
          }
          break;
        default:
          break;
        }
      }
      return deltaRecord;
    }

    @Override
    public BeamSqlRow mergeAccumulators(Iterable<BeamSqlRow> accumulators) {
      BeamSqlRow deltaRecord = new BeamSqlRow(aggDataType);

      while (accumulators.iterator().hasNext()) {
        BeamSqlRow sa = accumulators.iterator().next();
        for (int idx = 0; idx < aggElementExpressions.size(); ++idx) {
          BeamSqlExpression ex = aggElementExpressions.get(idx);
          String aggFnName = aggFunctions.get(idx);
          switch (aggFnName) {
          case "COUNT":
            deltaRecord.addField(idx, deltaRecord.getLong(idx) + sa.getLong(idx));
            break;
          case "AVG":
          case "SUM":
            // for both AVG/SUM, a summary value is hold at first.
            switch (ex.getOutputType()) {
            case INTEGER:
              deltaRecord.addField(idx, deltaRecord.getInteger(idx) + sa.getInteger(idx));
              break;
            case BIGINT:
              deltaRecord.addField(idx, deltaRecord.getLong(idx) + sa.getLong(idx));
              break;
            case SMALLINT:
              deltaRecord.addField(idx, (short) (deltaRecord.getShort(idx) + sa.getShort(idx)));
              break;
            case TINYINT:
              deltaRecord.addField(idx, (byte) (deltaRecord.getByte(idx) + sa.getByte(idx)));
              break;
            case FLOAT:
              deltaRecord.addField(idx, (float) (deltaRecord.getFloat(idx) + sa.getFloat(idx)));
              break;
            case DOUBLE:
              deltaRecord.addField(idx, deltaRecord.getDouble(idx) + sa.getDouble(idx));
              break;
            default:
              break;
            }
            break;
          case "MAX":
            switch (ex.getOutputType()) {
            case INTEGER:
              deltaRecord.addField(idx, Math.max(deltaRecord.getInteger(idx), sa.getInteger(idx)));
              break;
            case BIGINT:
              deltaRecord.addField(idx, Math.max(deltaRecord.getLong(idx), sa.getLong(idx)));
              break;
            case SMALLINT:
              deltaRecord.addField(idx,
                  (short) Math.max(deltaRecord.getShort(idx), sa.getShort(idx)));
              break;
            case TINYINT:
              deltaRecord.addField(idx, (byte) Math.max(deltaRecord.getByte(idx), sa.getByte(idx)));
              break;
            case FLOAT:
              deltaRecord.addField(idx, Math.max(deltaRecord.getFloat(idx), sa.getFloat(idx)));
              break;
            case DOUBLE:
              deltaRecord.addField(idx, Math.max(deltaRecord.getDouble(idx), sa.getDouble(idx)));
              break;
            case TIMESTAMP:
              Date preDate = deltaRecord.getDate(idx);
              Date nowDate = sa.getDate(idx);
              deltaRecord.addField(idx, preDate.getTime() > nowDate.getTime() ? preDate : nowDate);
              break;
            default:
              break;
            }
            break;
          case "MIN":
            switch (ex.getOutputType()) {
            case INTEGER:
              deltaRecord.addField(idx, Math.min(deltaRecord.getInteger(idx), sa.getInteger(idx)));
              break;
            case BIGINT:
              deltaRecord.addField(idx, Math.min(deltaRecord.getLong(idx), sa.getLong(idx)));
              break;
            case SMALLINT:
              deltaRecord.addField(idx,
                  (short) Math.min(deltaRecord.getShort(idx), sa.getShort(idx)));
              break;
            case TINYINT:
              deltaRecord.addField(idx, (byte) Math.min(deltaRecord.getByte(idx), sa.getByte(idx)));
              break;
            case FLOAT:
              deltaRecord.addField(idx, Math.min(deltaRecord.getFloat(idx), sa.getFloat(idx)));
              break;
            case DOUBLE:
              deltaRecord.addField(idx, Math.min(deltaRecord.getDouble(idx), sa.getDouble(idx)));
              break;
            case TIMESTAMP:
              Date preDate = deltaRecord.getDate(idx);
              Date nowDate = sa.getDate(idx);
              deltaRecord.addField(idx, preDate.getTime() < nowDate.getTime() ? preDate : nowDate);
              break;
            default:
              break;
            }
            break;
          default:
            break;
          }
        }
      }
      return deltaRecord;
    }

    @Override
    public BeamSqlRow extractOutput(BeamSqlRow accumulator) {
      BeamSqlRow finalRecord = new BeamSqlRow(aggDataType);
      for (int idx = 0; idx < aggElementExpressions.size(); ++idx) {
        BeamSqlExpression ex = aggElementExpressions.get(idx);
        String aggFnName = aggFunctions.get(idx);
        switch (aggFnName) {
        case "COUNT":
          finalRecord.addField(idx, accumulator.getLong(idx));
          break;
        case "AVG":
          long count = accumulator.getLong(countIndex);
          switch (ex.getOutputType()) {
          case INTEGER:
            finalRecord.addField(idx, (int) (accumulator.getInteger(idx) / count));
            break;
          case BIGINT:
            finalRecord.addField(idx, accumulator.getLong(idx) / count);
            break;
          case SMALLINT:
            finalRecord.addField(idx, (short) (accumulator.getShort(idx) / count));
            break;
          case TINYINT:
            finalRecord.addField(idx, (byte) (accumulator.getByte(idx) / count));
            break;
          case FLOAT:
            finalRecord.addField(idx, (float) (accumulator.getFloat(idx) / count));
            break;
          case DOUBLE:
            finalRecord.addField(idx, accumulator.getDouble(idx) / count);
            break;
          default:
            break;
          }
          break;
        case "SUM":
          switch (ex.getOutputType()) {
          case INTEGER:
            finalRecord.addField(idx, accumulator.getInteger(idx));
            break;
          case BIGINT:
            finalRecord.addField(idx, accumulator.getLong(idx));
            break;
          case SMALLINT:
            finalRecord.addField(idx, accumulator.getShort(idx));
            break;
          case TINYINT:
            finalRecord.addField(idx, accumulator.getByte(idx));
            break;
          case FLOAT:
            finalRecord.addField(idx, accumulator.getFloat(idx));
            break;
          case DOUBLE:
            finalRecord.addField(idx, accumulator.getDouble(idx));
            break;
          default:
            break;
          }
          break;
        case "MAX":
        case "MIN":
          switch (ex.getOutputType()) {
          case INTEGER:
            finalRecord.addField(idx, accumulator.getInteger(idx));
            break;
          case BIGINT:
            finalRecord.addField(idx, accumulator.getLong(idx));
            break;
          case SMALLINT:
            finalRecord.addField(idx, accumulator.getShort(idx));
            break;
          case TINYINT:
            finalRecord.addField(idx, accumulator.getByte(idx));
            break;
          case FLOAT:
            finalRecord.addField(idx, accumulator.getFloat(idx));
            break;
          case DOUBLE:
            finalRecord.addField(idx, accumulator.getDouble(idx));
            break;
          case TIMESTAMP:
            finalRecord.addField(idx, accumulator.getDate(idx));
            break;
          default:
            break;
          }
          break;
        default:
          break;
        }
      }
      return finalRecord;
    }
  }
}
