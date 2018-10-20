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

import static org.apache.beam.sdk.extensions.sql.impl.transform.BeamBuiltinAggregations.BUILTIN_AGGREGATOR_FACTORIES;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.apache.beam.sdk.values.Row.toRow;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.sql.impl.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
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

    public AggregationGroupByKeyFn(Schema keySchema, List<Integer> groupByKeys) {
      this.keySchema = keySchema;
      this.groupByKeys = groupByKeys;
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
      this.windowFieldIdx = windowFieldIdx;
    }

    @Override
    public Instant apply(Row input) {
      return new Instant(input.getDateTime(windowFieldIdx));
    }
  }

  /** Wrapper for aggregation function call information. */
  @AutoValue
  public abstract static class AggregationCall implements Serializable {
    public abstract String functionName();

    public abstract Schema.Field field();

    public abstract List<Integer> args();

    public abstract @Nullable CombineFn<?, ?, ?> udafCombineFn();

    public static AggregationCall of(Pair<AggregateCall, String> callWithAlias) {
      AggregateCall call = callWithAlias.getKey();
      String alias = callWithAlias.getValue();

      return new AutoValue_BeamAggregationTransforms_AggregationCall(
          call.getAggregation().getName(),
          CalciteUtils.toField(alias, call.getType()),
          call.getArgList(),
          getUdafCombineFn(call));
    }

    private static @Nullable CombineFn<?, ?, ?> getUdafCombineFn(AggregateCall call) {
      if (!(call.getAggregation() instanceof SqlUserDefinedAggFunction)) {
        return null;
      }

      try {
        return ((UdafImpl) ((SqlUserDefinedAggFunction) call.getAggregation()).function)
            .getCombineFn();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /** An adaptor class to invoke Calcite UDAF instances in Beam {@code CombineFn}. */
  public static class AggregationAdaptor extends CombineFn<Row, AggregationAccumulator, Row> {
    private List<CombineFn> aggregators;
    private List<Object> sourceFieldExps;
    private Schema sourceSchema;
    private Schema finalSchema;

    public AggregationAdaptor(List<AggregationCall> aggregationCalls, Schema sourceSchema) {
      this.aggregators = new ArrayList<>();
      this.sourceFieldExps = new ArrayList<>();
      this.sourceSchema = sourceSchema;
      ImmutableList.Builder<Schema.Field> fields = ImmutableList.builder();

      for (AggregationCall aggCall : aggregationCalls) {

        if (aggCall.args().size() == 2) {
          /*
           * handle the case of aggregation function has two parameters and use KV pair to bundle
           * two corresponding expressions.
           */
          int refIndexKey = aggCall.args().get(0);
          int refIndexValue = aggCall.args().get(1);

          sourceFieldExps.add(KV.of(refIndexKey, refIndexValue));
        } else {
          Integer refIndex = aggCall.args().size() > 0 ? aggCall.args().get(0) : 0;
          sourceFieldExps.add(refIndex);
        }

        fields.add(aggCall.field());
        aggregators.add(createAggregator(aggCall, aggCall.field().getType().getTypeName()));
      }
      finalSchema = fields.build().stream().collect(toSchema());
    }

    private CombineFn<?, ?, ?> createAggregator(
        AggregationCall aggCall, Schema.TypeName fieldTypeName) {

      if (aggCall.udafCombineFn() != null) {
        return aggCall.udafCombineFn();
      }

      Function<Schema.TypeName, CombineFn<?, ?, ?>> aggregatorFactory =
          BUILTIN_AGGREGATOR_FACTORIES.get(aggCall.functionName());

      if (aggregatorFactory != null) {
        return aggregatorFactory.apply(fieldTypeName);
      }

      throw new UnsupportedOperationException(
          String.format("Aggregator [%s] is not supported", aggCall.functionName()));
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

        if (sourceFieldExps.get(idx) instanceof Integer) {
          Object value = input.getValue((Integer) sourceFieldExps.get(idx));

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
          KV<Integer, Integer> exp = (KV<Integer, Integer>) sourceFieldExps.get(idx);

          Object key = input.getValue(exp.getKey());

          Object value = input.getValue(exp.getValue());

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
        if (sourceFieldExps.get(idx) instanceof Integer) {
          int srcFieldIndex = (Integer) sourceFieldExps.get(idx);
          Coder srcFieldCoder =
              RowCoder.coderForFieldType(sourceSchema.getField(srcFieldIndex).getType());
          aggAccuCoderList.add(aggregators.get(idx).getAccumulatorCoder(registry, srcFieldCoder));
        } else if (sourceFieldExps.get(idx) instanceof KV) {
          // extract coder of two expressions separately.
          KV<Integer, Integer> exp = (KV<Integer, Integer>) sourceFieldExps.get(idx);

          int srcFieldIndexKey = exp.getKey();
          int srcFieldIndexValue = exp.getValue();

          Coder srcFieldCoderKey =
              RowCoder.coderForFieldType(sourceSchema.getField(srcFieldIndexKey).getType());
          Coder srcFieldCoderValue =
              RowCoder.coderForFieldType(sourceSchema.getField(srcFieldIndexValue).getType());

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
