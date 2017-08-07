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
package org.apache.beam.sdk.extensions.sql.impl.transform;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.schema.BeamSqlRecordHelper;
import org.apache.beam.sdk.extensions.sql.schema.BeamSqlRecordType;
import org.apache.beam.sdk.extensions.sql.schema.BeamSqlUdaf;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.BeamRecord;
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
  public static class MergeAggregationRecord extends DoFn<KV<BeamRecord, BeamRecord>, BeamRecord> {
    private BeamSqlRecordType outRowType;
    private List<String> aggFieldNames;
    private int windowStartFieldIdx;

    public MergeAggregationRecord(BeamSqlRecordType outRowType, List<AggregateCall> aggList
        , int windowStartFieldIdx) {
      this.outRowType = outRowType;
      this.aggFieldNames = new ArrayList<>();
      for (AggregateCall ac : aggList) {
        aggFieldNames.add(ac.getName());
      }
      this.windowStartFieldIdx = windowStartFieldIdx;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      List<Object> fieldValues = new ArrayList<>();
      KV<BeamRecord, BeamRecord> kvRecord = c.element();
      fieldValues.addAll(kvRecord.getKey().getDataValues());
      fieldValues.addAll(kvRecord.getValue().getDataValues());
      if (windowStartFieldIdx != -1) {
        fieldValues.add(windowStartFieldIdx, ((IntervalWindow) window).start().toDate());
      }

      BeamRecord outRecord = new BeamRecord(outRowType, fieldValues);
      c.output(outRecord);
    }
  }

  /**
   * extract group-by fields.
   */
  public static class AggregationGroupByKeyFn
      implements SerializableFunction<BeamRecord, BeamRecord> {
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
    public BeamRecord apply(BeamRecord input) {
      BeamSqlRecordType typeOfKey = exTypeOfKeyRecord(BeamSqlRecordHelper.getSqlRecordType(input));

      List<Object> fieldValues = new ArrayList<>();
      for (int idx = 0; idx < groupByKeys.size(); ++idx) {
        fieldValues.add(input.getFieldValue(groupByKeys.get(idx)));
      }

      BeamRecord keyOfRecord = new BeamRecord(typeOfKey, fieldValues);
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
  public static class WindowTimestampFn implements SerializableFunction<BeamRecord, Instant> {
    private int windowFieldIdx = -1;

    public WindowTimestampFn(int windowFieldIdx) {
      super();
      this.windowFieldIdx = windowFieldIdx;
    }

    @Override
    public Instant apply(BeamRecord input) {
      return new Instant(input.getDate(windowFieldIdx).getTime());
    }
  }

  /**
   * An adaptor class to invoke Calcite UDAF instances in Beam {@code CombineFn}.
   */
  public static class AggregationAdaptor
    extends CombineFn<BeamRecord, AggregationAccumulator, BeamRecord> {
    private List<BeamSqlUdaf> aggregators;
    private List<BeamSqlExpression> sourceFieldExps;
    private BeamSqlRecordType finalRowType;

    public AggregationAdaptor(List<AggregateCall> aggregationCalls,
        BeamSqlRecordType sourceRowType) {
      aggregators = new ArrayList<>();
      sourceFieldExps = new ArrayList<>();
      List<String> outFieldsName = new ArrayList<>();
      List<Integer> outFieldsType = new ArrayList<>();
      for (AggregateCall call : aggregationCalls) {
        int refIndex = call.getArgList().size() > 0 ? call.getArgList().get(0) : 0;
        BeamSqlExpression sourceExp = new BeamSqlInputRefExpression(
            CalciteUtils.getFieldType(sourceRowType, refIndex), refIndex);
        sourceFieldExps.add(sourceExp);

        outFieldsName.add(call.name);
        int outFieldType = CalciteUtils.toJavaType(call.type.getSqlTypeName());
        outFieldsType.add(outFieldType);

        switch (call.getAggregation().getName()) {
          case "COUNT":
            aggregators.add(new BeamBuiltinAggregations.Count());
            break;
          case "MAX":
            aggregators.add(BeamBuiltinAggregations.Max.create(call.type.getSqlTypeName()));
            break;
          case "MIN":
            aggregators.add(BeamBuiltinAggregations.Min.create(call.type.getSqlTypeName()));
            break;
          case "SUM":
            aggregators.add(BeamBuiltinAggregations.Sum.create(call.type.getSqlTypeName()));
            break;
          case "AVG":
            aggregators.add(BeamBuiltinAggregations.Avg.create(call.type.getSqlTypeName()));
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
              throw new UnsupportedOperationException(
                  String.format("Aggregator [%s] is not supported",
                  call.getAggregation().getName()));
            }
          break;
        }
      }
      finalRowType = BeamSqlRecordType.create(outFieldsName, outFieldsType);
    }
    @Override
    public AggregationAccumulator createAccumulator() {
      AggregationAccumulator initialAccu = new AggregationAccumulator();
      for (BeamSqlUdaf agg : aggregators) {
        initialAccu.accumulatorElements.add(agg.init());
      }
      return initialAccu;
    }
    @Override
    public AggregationAccumulator addInput(AggregationAccumulator accumulator, BeamRecord input) {
      AggregationAccumulator deltaAcc = new AggregationAccumulator();
      for (int idx = 0; idx < aggregators.size(); ++idx) {
        deltaAcc.accumulatorElements.add(
            aggregators.get(idx).add(accumulator.accumulatorElements.get(idx),
            sourceFieldExps.get(idx).evaluate(input, null).getValue()));
      }
      return deltaAcc;
    }
    @Override
    public AggregationAccumulator mergeAccumulators(Iterable<AggregationAccumulator> accumulators) {
      AggregationAccumulator deltaAcc = new AggregationAccumulator();
      for (int idx = 0; idx < aggregators.size(); ++idx) {
        List accs = new ArrayList<>();
        Iterator<AggregationAccumulator> ite = accumulators.iterator();
        while (ite.hasNext()) {
          accs.add(ite.next().accumulatorElements.get(idx));
        }
        deltaAcc.accumulatorElements.add(aggregators.get(idx).merge(accs));
      }
      return deltaAcc;
    }
    @Override
    public BeamRecord extractOutput(AggregationAccumulator accumulator) {
      List<Object> fieldValues = new ArrayList<>();
      for (int idx = 0; idx < aggregators.size(); ++idx) {
        fieldValues.add(aggregators.get(idx).result(accumulator.accumulatorElements.get(idx)));
      }
      return new BeamRecord(finalRowType, fieldValues);
    }
    @Override
    public Coder<AggregationAccumulator> getAccumulatorCoder(
        CoderRegistry registry, Coder<BeamRecord> inputCoder)
        throws CannotProvideCoderException {
      registry.registerCoderForClass(BigDecimal.class, BigDecimalCoder.of());
      List<Coder> aggAccuCoderList = new ArrayList<>();
      for (BeamSqlUdaf udaf : aggregators) {
        aggAccuCoderList.add(udaf.getAccumulatorCoder(registry));
      }
      return new AggregationAccumulatorCoder(aggAccuCoderList);
    }
  }

  /**
   * A class to holder varied accumulator objects.
   */
  public static class AggregationAccumulator{
    private List accumulatorElements = new ArrayList<>();
  }

  /**
   * Coder for {@link AggregationAccumulator}.
   */
  public static class AggregationAccumulatorCoder extends CustomCoder<AggregationAccumulator>{
    private VarIntCoder sizeCoder = VarIntCoder.of();
    private List<Coder> elementCoders;

    public AggregationAccumulatorCoder(List<Coder> elementCoders) {
      this.elementCoders = elementCoders;
    }

    @Override
    public void encode(AggregationAccumulator value, OutputStream outStream)
        throws CoderException, IOException {
      sizeCoder.encode(value.accumulatorElements.size(), outStream);
      for (int idx = 0; idx < value.accumulatorElements.size(); ++idx) {
        elementCoders.get(idx).encode(value.accumulatorElements.get(idx), outStream);
      }
    }

    @Override
    public AggregationAccumulator decode(InputStream inStream) throws CoderException, IOException {
      AggregationAccumulator accu = new AggregationAccumulator();
      int size = sizeCoder.decode(inStream);
      for (int idx = 0; idx < size; ++idx) {
        accu.accumulatorElements.add(elementCoders.get(idx).decode(inStream));
      }
      return accu;
    }
  }
}
