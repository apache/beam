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

import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.apache.beam.sdk.values.Row.toRow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironments;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.CovarianceFn;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.VarianceFn;
import org.apache.beam.sdk.extensions.sql.impl.utils.BigDecimalConverter;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.joda.time.Instant;

/** Collections of {@code PTransform} and {@code DoFn} used to perform GROUP-BY operation. */
public class BeamAggregationTransforms implements Serializable {
  /** Merge KV to single record. */
  public static class MergeAggregationRecord extends DoFn<KV<Row, Row>, Row> {
    private Schema outSchema;
    private int windowStartFieldIdx;

    public MergeAggregationRecord(Schema outSchema, int windowStartFieldIdx) {
      this.outSchema = outSchema;
      this.windowStartFieldIdx = windowStartFieldIdx;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      KV<Row, Row> kvRow = c.element();
      List<Object> fieldValues =
          Lists.newArrayListWithCapacity(
              kvRow.getKey().getValues().size() + kvRow.getValue().getValues().size());
      fieldValues.addAll(kvRow.getKey().getValues());
      fieldValues.addAll(kvRow.getValue().getValues());

      if (windowStartFieldIdx != -1) {
        fieldValues.add(windowStartFieldIdx, ((IntervalWindow) window).start());
      }

      c.output(Row.withSchema(outSchema).addValues(fieldValues).build());
    }
  }

  /** extract group-by fields. */
  public static class AggregationGroupByKeyFn implements SerializableFunction<Row, Row> {
    private Schema keySchema;
    private List<Integer> groupByKeys;

    public AggregationGroupByKeyFn(Schema keySchema, int windowFieldIdx, ImmutableBitSet groupSet) {
      this.keySchema = keySchema;
      this.groupByKeys = new ArrayList<>();
      for (int i : groupSet.asList()) {
        if (i != windowFieldIdx) {
          groupByKeys.add(i);
        }
      }
    }

    @Override
    public Row apply(Row input) {
      return groupByKeys.stream().map(input::getValue).collect(toRow(keySchema));
    }
  }

  /** Assign event timestamp. */
  public static class WindowTimestampFn implements SerializableFunction<Row, Instant> {
    private int windowFieldIdx = -1;

    public WindowTimestampFn(int windowFieldIdx) {
      super();
      this.windowFieldIdx = windowFieldIdx;
    }

    @Override
    public Instant apply(Row input) {
      return new Instant(input.getDateTime(windowFieldIdx));
    }
  }

  /** An adaptor class to invoke Calcite UDAF instances in Beam {@code CombineFn}. */
  public static class AggregationAdaptor extends CombineFn<Row, AggregationAccumulator, Row> {
    private List<CombineFn> aggregators;
    private List<Object> sourceFieldExps;
    private Schema sourceSchema;
    private Schema finalSchema;

    public AggregationAdaptor(
        List<Pair<AggregateCall, String>> aggregationCalls, Schema sourceSchema) {
      this.aggregators = new ArrayList<>();
      this.sourceFieldExps = new ArrayList<>();
      this.sourceSchema = sourceSchema;
      ImmutableList.Builder<Schema.Field> fields = ImmutableList.builder();

      for (Pair<AggregateCall, String> aggCall : aggregationCalls) {
        AggregateCall call = aggCall.left;
        String aggName = aggCall.right;

        if (call.getArgList().size() == 2) {
          /*
           * handle the case of aggregation function has two parameters and use KV pair to bundle
           * two corresponding expressions.
           */
          int refIndexKey = call.getArgList().get(0);
          int refIndexValue = call.getArgList().get(1);

          FieldType keyDescriptor = sourceSchema.getField(refIndexKey).getType();
          BeamSqlInputRefExpression sourceExpKey =
              new BeamSqlInputRefExpression(CalciteUtils.toSqlTypeName(keyDescriptor), refIndexKey);

          FieldType valueDescriptor = sourceSchema.getField(refIndexValue).getType();
          BeamSqlInputRefExpression sourceExpValue =
              new BeamSqlInputRefExpression(
                  CalciteUtils.toSqlTypeName(valueDescriptor), refIndexValue);

          sourceFieldExps.add(KV.of(sourceExpKey, sourceExpValue));
        } else {
          int refIndex = call.getArgList().size() > 0 ? call.getArgList().get(0) : 0;
          FieldType fieldType = sourceSchema.getField(refIndex).getType();
          BeamSqlInputRefExpression sourceExp =
              new BeamSqlInputRefExpression(CalciteUtils.toSqlTypeName(fieldType), refIndex);
          sourceFieldExps.add(sourceExp);
        }

        Schema.Field field = CalciteUtils.toField(aggName, call.type);
        Schema.TypeName fieldTypeName = field.getType().getTypeName();
        fields.add(field);

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
          case "$SUM0":
            aggregators.add(BeamBuiltinAggregations.createSum(call.type.getSqlTypeName()));
            break;
          case "AVG":
            aggregators.add(BeamBuiltinAggregations.createAvg(call.type.getSqlTypeName()));
            break;
          case "VAR_POP":
            aggregators.add(
                VarianceFn.newPopulation(BigDecimalConverter.forSqlType(fieldTypeName)));
            break;
          case "VAR_SAMP":
            aggregators.add(VarianceFn.newSample(BigDecimalConverter.forSqlType(fieldTypeName)));
            break;
          case "COVAR_POP":
            aggregators.add(
                CovarianceFn.newPopulation(BigDecimalConverter.forSqlType(fieldTypeName)));
            break;
          case "COVAR_SAMP":
            aggregators.add(CovarianceFn.newSample(BigDecimalConverter.forSqlType(fieldTypeName)));
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
                  String.format(
                      "Aggregator [%s] is not supported", call.getAggregation().getName()));
            }
            break;
        }
      }
      finalSchema = fields.build().stream().collect(toSchema());
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
    public AggregationAccumulator addInput(AggregationAccumulator accumulator, Row input) {
      AggregationAccumulator deltaAcc = new AggregationAccumulator();
      for (int idx = 0; idx < aggregators.size(); ++idx) {
        CombineFn aggregator = aggregators.get(idx);
        Object element = accumulator.accumulatorElements.get(idx);

        if (sourceFieldExps.get(idx) instanceof BeamSqlInputRefExpression) {
          BeamSqlInputRefExpression exp = (BeamSqlInputRefExpression) sourceFieldExps.get(idx);
          Object value =
              exp.evaluate(input, null, BeamSqlExpressionEnvironments.empty()).getValue();

          // every aggregator ignores null values, e.g., COUNT(NULL) is always zero
          if (value != null) {
            Object delta = aggregator.addInput(element, value);
            deltaAcc.accumulatorElements.add(delta);
          } else {
            deltaAcc.accumulatorElements.add(element);
          }
        } else if (sourceFieldExps.get(idx) instanceof KV) {
          /*
           * If source expression is type of KV pair, we bundle the value of two expressions into KV
           * pair and pass it to aggregator's addInput method.
           */
          KV<BeamSqlInputRefExpression, BeamSqlInputRefExpression> exp =
              (KV<BeamSqlInputRefExpression, BeamSqlInputRefExpression>) sourceFieldExps.get(idx);

          Object key =
              exp.getKey().evaluate(input, null, BeamSqlExpressionEnvironments.empty()).getValue();

          Object value =
              exp.getValue()
                  .evaluate(input, null, BeamSqlExpressionEnvironments.empty())
                  .getValue();

          // ignore aggregator if either key or value is null, e.g., COVAR_SAMP(x, NULL) is null
          if (key != null && value != null) {
            deltaAcc.accumulatorElements.add(aggregator.addInput(element, KV.of(key, value)));
          } else {
            deltaAcc.accumulatorElements.add(element);
          }
        }
      }
      return deltaAcc;
    }

    @Override
    public AggregationAccumulator mergeAccumulators(Iterable<AggregationAccumulator> accumulators) {
      AggregationAccumulator deltaAcc = new AggregationAccumulator();
      for (int idx = 0; idx < aggregators.size(); ++idx) {
        List accs = new ArrayList<>();
        for (AggregationAccumulator accumulator : accumulators) {
          accs.add(accumulator.accumulatorElements.get(idx));
        }
        deltaAcc.accumulatorElements.add(aggregators.get(idx).mergeAccumulators(accs));
      }
      return deltaAcc;
    }

    @Override
    public Row extractOutput(AggregationAccumulator accumulator) {
      return IntStream.range(0, aggregators.size())
          .mapToObj(idx -> getAggregatorOutput(accumulator, idx))
          .collect(toRow(finalSchema));
    }

    private Object getAggregatorOutput(AggregationAccumulator accumulator, int idx) {
      return aggregators.get(idx).extractOutput(accumulator.accumulatorElements.get(idx));
    }

    @Override
    public Coder<AggregationAccumulator> getAccumulatorCoder(
        CoderRegistry registry, Coder<Row> inputCoder) throws CannotProvideCoderException {
      // TODO: Doing this here is wrong.
      registry.registerCoderForClass(BigDecimal.class, BigDecimalCoder.of());
      List<Coder> aggAccuCoderList = new ArrayList<>();
      for (int idx = 0; idx < aggregators.size(); ++idx) {
        if (sourceFieldExps.get(idx) instanceof BeamSqlInputRefExpression) {
          BeamSqlInputRefExpression exp = (BeamSqlInputRefExpression) sourceFieldExps.get(idx);
          int srcFieldIndex = exp.getInputRef();
          Coder srcFieldCoder =
              RowCoder.coderForPrimitiveType(
                  sourceSchema.getField(srcFieldIndex).getType().getTypeName());
          aggAccuCoderList.add(aggregators.get(idx).getAccumulatorCoder(registry, srcFieldCoder));
        } else if (sourceFieldExps.get(idx) instanceof KV) {
          // extract coder of two expressions separately.
          KV<BeamSqlInputRefExpression, BeamSqlInputRefExpression> exp =
              (KV<BeamSqlInputRefExpression, BeamSqlInputRefExpression>) sourceFieldExps.get(idx);

          int srcFieldIndexKey = exp.getKey().getInputRef();
          int srcFieldIndexValue = exp.getValue().getInputRef();

          Coder srcFieldCoderKey =
              RowCoder.coderForPrimitiveType(
                  sourceSchema.getField(srcFieldIndexKey).getType().getTypeName());
          Coder srcFieldCoderValue =
              RowCoder.coderForPrimitiveType(
                  sourceSchema.getField(srcFieldIndexValue).getType().getTypeName());

          aggAccuCoderList.add(
              aggregators
                  .get(idx)
                  .getAccumulatorCoder(registry, KvCoder.of(srcFieldCoderKey, srcFieldCoderValue)));
        }
      }
      return new AggregationAccumulatorCoder(aggAccuCoderList);
    }
  }

  /** A class to holder varied accumulator objects. */
  public static class AggregationAccumulator {
    private List accumulatorElements = new ArrayList<>();
  }

  /** Coder for {@link AggregationAccumulator}. */
  public static class AggregationAccumulatorCoder extends CustomCoder<AggregationAccumulator> {
    private VarIntCoder sizeCoder = VarIntCoder.of();
    private List<Coder> elementCoders;

    public AggregationAccumulatorCoder(List<Coder> elementCoders) {
      this.elementCoders = elementCoders;
    }

    @Override
    public void encode(AggregationAccumulator value, OutputStream outStream) throws IOException {
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
