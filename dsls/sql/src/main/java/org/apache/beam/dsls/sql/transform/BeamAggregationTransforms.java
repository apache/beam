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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlUdaf;
import org.apache.beam.dsls.sql.utils.CalciteUtils;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
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
      List<String> fieldNames = new ArrayList<>();
      List<Integer> fieldTypes = new ArrayList<>();
      for (int idx : groupByKeys) {
        fieldNames.add(dataType.getFieldsName().get(idx));
        fieldTypes.add(dataType.getFieldsType().get(idx));
      }
      return BeamSqlRecordType.create(fieldNames, fieldTypes);
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
   * An adaptor class to invoke Calcite UDAF instances in Beam {@code CombineFn}.
   */
  public static class AggregationAdaptor
    extends CombineFn<BeamSqlRow, List<BeamSqlRow>, BeamSqlRow> {
    private List<BeamSqlUdaf> aggregators;
    private List<BeamSqlExpression> sourceFieldExps;
    private BeamSqlRecordType finalRecordType;

    public AggregationAdaptor(List<AggregateCall> aggregationCalls,
        BeamSqlRecordType sourceRowRecordType) {
      aggregators = new ArrayList<>();
      sourceFieldExps = new ArrayList<>();
      List<String> outFieldsName = new ArrayList<>();
      List<Integer> outFieldsType = new ArrayList<>();
      for (AggregateCall call : aggregationCalls) {
        int refIndex = call.getArgList().size() > 0 ? call.getArgList().get(0) : 0;
        BeamSqlExpression sourceExp = new BeamSqlInputRefExpression(
            CalciteUtils.getFieldType(sourceRowRecordType, refIndex), refIndex);
        sourceFieldExps.add(sourceExp);

        outFieldsName.add(call.name);
        int outFieldType = CalciteUtils.toJavaType(call.type.getSqlTypeName());
        outFieldsType.add(outFieldType);

        switch (call.getAggregation().getName()) {
        case "COUNT":
          aggregators.add(new BeamBuiltinAggregations.Count());
          break;
        case "MAX":
          switch (call.type.getSqlTypeName()) {
          case INTEGER:
            aggregators.add(new BeamBuiltinAggregations.Max<Integer>(outFieldType));
            break;
          case SMALLINT:
            aggregators.add(new BeamBuiltinAggregations.Max<Short>(outFieldType));
            break;
          case TINYINT:
            aggregators.add(new BeamBuiltinAggregations.Max<Byte>(outFieldType));
            break;
          case BIGINT:
            aggregators.add(new BeamBuiltinAggregations.Max<Long>(outFieldType));
            break;
          case FLOAT:
            aggregators.add(new BeamBuiltinAggregations.Max<Float>(outFieldType));
            break;
          case DOUBLE:
            aggregators.add(new BeamBuiltinAggregations.Max<Double>(outFieldType));
            break;
          case TIMESTAMP:
            aggregators.add(new BeamBuiltinAggregations.Max<Date>(outFieldType));
            break;
          case DECIMAL:
            aggregators.add(new BeamBuiltinAggregations.Max<BigDecimal>(outFieldType));
            break;
          default:
            throw new UnsupportedOperationException();
          }
          break;
        case "MIN":
          switch (call.type.getSqlTypeName()) {
          case INTEGER:
            aggregators.add(new BeamBuiltinAggregations.Min<Integer>(outFieldType));
            break;
          case SMALLINT:
            aggregators.add(new BeamBuiltinAggregations.Min<Short>(outFieldType));
            break;
          case TINYINT:
            aggregators.add(new BeamBuiltinAggregations.Min<Byte>(outFieldType));
            break;
          case BIGINT:
            aggregators.add(new BeamBuiltinAggregations.Min<Long>(outFieldType));
            break;
          case FLOAT:
            aggregators.add(new BeamBuiltinAggregations.Min<Float>(outFieldType));
            break;
          case DOUBLE:
            aggregators.add(new BeamBuiltinAggregations.Min<Double>(outFieldType));
            break;
          case TIMESTAMP:
            aggregators.add(new BeamBuiltinAggregations.Min<Date>(outFieldType));
            break;
          case DECIMAL:
            aggregators.add(new BeamBuiltinAggregations.Min<BigDecimal>(outFieldType));
            break;
          default:
            throw new UnsupportedOperationException();
          }
          break;
        case "SUM":
          switch (call.type.getSqlTypeName()) {
          case INTEGER:
            aggregators.add(new BeamBuiltinAggregations.Sum<Integer>(outFieldType));
            break;
          case SMALLINT:
            aggregators.add(new BeamBuiltinAggregations.Sum<Short>(outFieldType));
            break;
          case TINYINT:
            aggregators.add(new BeamBuiltinAggregations.Sum<Byte>(outFieldType));
            break;
          case BIGINT:
            aggregators.add(new BeamBuiltinAggregations.Sum<Long>(outFieldType));
            break;
          case FLOAT:
            aggregators.add(new BeamBuiltinAggregations.Sum<Float>(outFieldType));
            break;
          case DOUBLE:
            aggregators.add(new BeamBuiltinAggregations.Sum<Double>(outFieldType));
            break;
          case DECIMAL:
            aggregators.add(new BeamBuiltinAggregations.Sum<BigDecimal>(outFieldType));
            break;
          default:
            throw new UnsupportedOperationException();
          }
          break;
        case "AVG":
          switch (call.type.getSqlTypeName()) {
          case INTEGER:
            aggregators.add(new BeamBuiltinAggregations.Avg<Integer>(outFieldType));
            break;
          case SMALLINT:
            aggregators.add(new BeamBuiltinAggregations.Avg<Short>(outFieldType));
            break;
          case TINYINT:
            aggregators.add(new BeamBuiltinAggregations.Avg<Byte>(outFieldType));
            break;
          case BIGINT:
            aggregators.add(new BeamBuiltinAggregations.Avg<Long>(outFieldType));
            break;
          case FLOAT:
            aggregators.add(new BeamBuiltinAggregations.Avg<Float>(outFieldType));
            break;
          case DOUBLE:
            aggregators.add(new BeamBuiltinAggregations.Avg<Double>(outFieldType));
            break;
          case DECIMAL:
            aggregators.add(new BeamBuiltinAggregations.Avg<BigDecimal>(outFieldType));
            break;
          default:
            throw new UnsupportedOperationException();
          }
          break;
        default:
          if (call.getAggregation() instanceof SqlUserDefinedAggFunction) {
            // handle UDAF.
            SqlUserDefinedAggFunction udaf = (SqlUserDefinedAggFunction) call.getAggregation();
            AggregateFunctionImpl fn = (AggregateFunctionImpl) udaf.function;
            try {
              aggregators.add((BeamSqlUdaf) fn.declaringClass.newInstance());
            } catch (Exception e) {
              throw new IllegalStateException(e);
            }
          } else {
            throw new UnsupportedOperationException();
          }
        }
      }
      finalRecordType = BeamSqlRecordType.create(outFieldsName, outFieldsType);
    }
    @Override
    public List<BeamSqlRow> createAccumulator() {
      List<BeamSqlRow> initialAccu = new ArrayList<>();
      for (BeamSqlUdaf agg : aggregators) {
        initialAccu.add(agg.init());
      }
      return initialAccu;
    }
    @Override
    public List<BeamSqlRow> addInput(List<BeamSqlRow> accumulator, BeamSqlRow input) {
      List<BeamSqlRow> deltaAcc = new ArrayList<>();
      for (int idx = 0; idx < aggregators.size(); ++idx) {
        deltaAcc.add(aggregators.get(idx).add(accumulator.get(idx),
            sourceFieldExps.get(idx).evaluate(input).getValue()));
      }
      return deltaAcc;
    }
    @Override
    public List<BeamSqlRow> mergeAccumulators(Iterable<List<BeamSqlRow>> accumulators) {
      List<BeamSqlRow> deltaAcc = new ArrayList<>();
      for (int idx = 0; idx < aggregators.size(); ++idx) {
        List<BeamSqlRow> accs = new ArrayList<>();
        while (accumulators.iterator().hasNext()) {
          accs.add(accumulators.iterator().next().get(idx));
        }
        deltaAcc.add(aggregators.get(idx).merge(accs));
      }
      return deltaAcc;
    }
    @Override
    public BeamSqlRow extractOutput(List<BeamSqlRow> accumulator) {
      BeamSqlRow result = new BeamSqlRow(finalRecordType);
      for (int idx = 0; idx < aggregators.size(); ++idx) {
        result.addField(idx, aggregators.get(idx).result(accumulator.get(idx)));
      }
      return result;
    }
  }
}
