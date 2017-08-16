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
import org.apache.beam.sdk.coders.BeamRecordCoder;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.sql.BeamRecordSqlType;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.calcite.rel.core.AggregateCall;
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
    private BeamRecordSqlType outRowType;
    private List<String> aggFieldNames;
    private int windowStartFieldIdx;

    public MergeAggregationRecord(BeamRecordSqlType outRowType, List<AggregateCall> aggList
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
      KV<BeamRecord, BeamRecord> kvRecord = c.element();
      List<Object> fieldValues = new ArrayList<>();
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
      BeamRecordSqlType typeOfKey = exTypeOfKeyRecord(BeamSqlRecordHelper.getSqlRecordType(input));

      List<Object> fieldValues = new ArrayList<>(groupByKeys.size());
      for (int idx = 0; idx < groupByKeys.size(); ++idx) {
        fieldValues.add(input.getFieldValue(groupByKeys.get(idx)));
      }

      BeamRecord keyOfRecord = new BeamRecord(typeOfKey, fieldValues);
      return keyOfRecord;
    }

    private BeamRecordSqlType exTypeOfKeyRecord(BeamRecordSqlType dataType) {
      List<String> fieldNames = new ArrayList<>();
      List<Integer> fieldTypes = new ArrayList<>();
      for (int idx : groupByKeys) {
        fieldNames.add(dataType.getFieldNameByIndex(idx));
        fieldTypes.add(dataType.getFieldTypeByIndex(idx));
      }
      return BeamRecordSqlType.create(fieldNames, fieldTypes);
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
    private List<CombineFn> aggregators;
    private List<BeamSqlInputRefExpression> sourceFieldExps;
    private BeamRecordSqlType finalRowType;

    public AggregationAdaptor(List<AggregateCall> aggregationCalls,
        BeamRecordSqlType sourceRowType) {
      aggregators = new ArrayList<>();
      sourceFieldExps = new ArrayList<>();
      List<String> outFieldsName = new ArrayList<>();
      List<Integer> outFieldsType = new ArrayList<>();
      for (AggregateCall call : aggregationCalls) {
        int refIndex = call.getArgList().size() > 0 ? call.getArgList().get(0) : 0;
        BeamSqlInputRefExpression sourceExp = new BeamSqlInputRefExpression(
            CalciteUtils.getFieldType(sourceRowType, refIndex), refIndex);
        sourceFieldExps.add(sourceExp);

        outFieldsName.add(call.name);
        int outFieldType = CalciteUtils.toJavaType(call.type.getSqlTypeName());
        outFieldsType.add(outFieldType);

        switch (call.getAggregation().getName()) {
          case "COUNT":
            aggregators.add(Count.combineFn());
            break;
          case "MAX":
            aggregators.add(BeamBuiltinAggregations.createMax(call.type.getSqlTypeName()));
            break;
          case "MIN":
            aggregators.add(BeamBuiltinAggregations.createMin(call.type.getSqlTypeName()));
            break;
          case "SUM":
            aggregators.add(BeamBuiltinAggregations.createSum(call.type.getSqlTypeName()));
            break;
          case "AVG":
            aggregators.add(BeamBuiltinAggregations.createAvg(call.type.getSqlTypeName()));
            break;
          case "VAR_POP":
            aggregators.add(BeamBuiltinAggregations.createVar(call.type.getSqlTypeName(),
                    false));
            break;
          case "VAR_SAMP":
            aggregators.add(BeamBuiltinAggregations.createVar(call.type.getSqlTypeName(),
                    true));
            break;
          default:
            if (call.getAggregation() instanceof SqlUserDefinedAggFunction) {
              // handle UDAF.
              SqlUserDefinedAggFunction udaf = (SqlUserDefinedAggFunction) call.getAggregation();
              UdafImpl fn = (UdafImpl) udaf.function;
              try {
                aggregators.add(fn.getCombineFn());
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
      finalRowType = BeamRecordSqlType.create(outFieldsName, outFieldsType);
    }
    @Override
    public AggregationAccumulator createAccumulator() {
      AggregationAccumulator initialAccu = new AggregationAccumulator();
      for (CombineFn agg : aggregators) {
        initialAccu.accumulatorElements.add(agg.createAccumulator());
      }
      return initialAccu;
    }
    @Override
    public AggregationAccumulator addInput(AggregationAccumulator accumulator, BeamRecord input) {
      AggregationAccumulator deltaAcc = new AggregationAccumulator();
      for (int idx = 0; idx < aggregators.size(); ++idx) {
        deltaAcc.accumulatorElements.add(
            aggregators.get(idx).addInput(accumulator.accumulatorElements.get(idx),
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
        deltaAcc.accumulatorElements.add(aggregators.get(idx).mergeAccumulators(accs));
      }
      return deltaAcc;
    }
    @Override
    public BeamRecord extractOutput(AggregationAccumulator accumulator) {
      List<Object> fieldValues = new ArrayList<>(aggregators.size());
      for (int idx = 0; idx < aggregators.size(); ++idx) {
        fieldValues
            .add(aggregators.get(idx).extractOutput(accumulator.accumulatorElements.get(idx)));
      }
      return new BeamRecord(finalRowType, fieldValues);
    }
    @Override
    public Coder<AggregationAccumulator> getAccumulatorCoder(
        CoderRegistry registry, Coder<BeamRecord> inputCoder)
        throws CannotProvideCoderException {
      BeamRecordCoder beamRecordCoder = (BeamRecordCoder) inputCoder;
      registry.registerCoderForClass(BigDecimal.class, BigDecimalCoder.of());
      List<Coder> aggAccuCoderList = new ArrayList<>();
      for (int idx = 0; idx < aggregators.size(); ++idx) {
        int srcFieldIndex = sourceFieldExps.get(idx).getInputRef();
        Coder srcFieldCoder = beamRecordCoder.getCoders().get(srcFieldIndex);
        aggAccuCoderList.add(aggregators.get(idx).getAccumulatorCoder(registry, srcFieldCoder));
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
