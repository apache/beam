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
package org.apache.beam.sdk.extensions.sql.impl.transform.agg;

import static org.apache.beam.sdk.extensions.sql.impl.transform.BeamBuiltinAggregations.BUILTIN_AGGREGATOR_FACTORIES;

import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.impl.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.Pair;

/**
 * Wrapper for aggregation function call. This is needed to avoid dealing with non-serializable
 * Calcite classes.
 */
public class AggregationCombineFnAdapter extends Combine.CombineFn<Row, Object, Object> {

  protected Schema.Field field;

  protected List<Integer> args;

  protected Combine.CombineFn combineFn;

  protected Schema sourceSchema;

  public Schema.Field field() {
    return field;
  }

  public AggregationCombineFnAdapter(
      Schema.Field field, List<Integer> args, Combine.CombineFn combineFn, Schema sourceSchema) {
    this.field = field;
    this.args = args;
    this.combineFn = combineFn;
    this.sourceSchema = sourceSchema;
  }

  public static AggregationCombineFnAdapter of(
      Pair<AggregateCall, String> callWithAlias, Schema inputSchema) {
    AggregateCall call = callWithAlias.getKey();
    Schema.Field field = CalciteUtils.toField(callWithAlias.getValue(), call.getType());
    String functionName = call.getAggregation().getName();

    return new AggregationCombineFnAdapter(
        field, call.getArgList(), createCombineFn(call, field, functionName), inputSchema);
  }

  private static Combine.CombineFn<?, ?, ?> createCombineFn(
      AggregateCall call, Schema.Field field, String functionName) {
    if (call.getAggregation() instanceof SqlUserDefinedAggFunction) {
      return getUdafCombineFn(call);
    }

    return createAggregator(functionName, field.getType().getTypeName());
  }

  private static Combine.CombineFn<?, ?, ?> getUdafCombineFn(AggregateCall call) {
    try {
      return ((UdafImpl) ((SqlUserDefinedAggFunction) call.getAggregation()).function)
          .getCombineFn();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static Combine.CombineFn<?, ?, ?> createAggregator(
      String functionName, Schema.TypeName fieldTypeName) {

    Function<Schema.TypeName, Combine.CombineFn<?, ?, ?>> aggregatorFactory =
        BUILTIN_AGGREGATOR_FACTORIES.get(functionName);

    if (aggregatorFactory != null) {
      return aggregatorFactory.apply(fieldTypeName);
    }

    throw new UnsupportedOperationException(
        String.format("Aggregator [%s] is not supported", functionName));
  }

  @Override
  public Object createAccumulator() {
    return combineFn.createAccumulator();
  }

  @Override
  public Object addInput(Object accumulator, Row input) {
    if (args.size() == 2) {
      // Aggregation function takes KV
      Object key = input.getValue(args.get(0));
      Object value = input.getValue(args.get(1));

      return (key == null || value == null)
          ? accumulator
          : combineFn.addInput(accumulator, KV.of(key, value));
    } else {
      // Aggregation function takes single value
      int fieldIndex = args.size() == 0 ? 0 : args.get(0);
      Object value = input.getValue(fieldIndex);
      return (value == null) ? accumulator : combineFn.addInput(accumulator, value);
    }
  }

  @Override
  public Object mergeAccumulators(Iterable<Object> accumulators) {
    return combineFn.mergeAccumulators(accumulators);
  }

  @Override
  public Object extractOutput(Object accumulator) {
    return combineFn.extractOutput(accumulator);
  }

  @Override
  public Coder<Object> getAccumulatorCoder(CoderRegistry registry, Coder<Row> inputCoder)
      throws CannotProvideCoderException {

    if (args.size() == 2) {
      int keyIndex = args.get(0);
      int valueIndex = args.get(1);

      Coder srcFieldCoderKey =
          RowCoder.coderForFieldType(sourceSchema.getField(keyIndex).getType());
      Coder srcFieldCoderValue =
          RowCoder.coderForFieldType(sourceSchema.getField(valueIndex).getType());

      return combineFn.getAccumulatorCoder(
          registry, KvCoder.of(srcFieldCoderKey, srcFieldCoderValue));
    } else {
      int fieldIndex = args.size() == 0 ? 0 : args.get(0);
      Coder srcFieldCoder = RowCoder.coderForFieldType(sourceSchema.getField(fieldIndex).getType());
      return combineFn.getAccumulatorCoder(registry, srcFieldCoder);
    }
  }
}
