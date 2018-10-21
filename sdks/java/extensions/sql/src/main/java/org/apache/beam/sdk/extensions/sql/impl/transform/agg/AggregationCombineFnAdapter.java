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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.extensions.sql.impl.transform.BeamBuiltinAggregations.BUILTIN_AGGREGATOR_FACTORIES;
import static org.apache.beam.sdk.schemas.Schema.toSchema;

import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.impl.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
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

    return createBuiltinCombineFn(functionName, field.getType().getTypeName());
  }

  private static Combine.CombineFn<?, ?, ?> getUdafCombineFn(AggregateCall call) {
    try {
      return ((UdafImpl) ((SqlUserDefinedAggFunction) call.getAggregation()).function)
          .getCombineFn();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static Combine.CombineFn<?, ?, ?> createBuiltinCombineFn(
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

    if (args.size() == 0) {
      Object value = input.getValue(0);
      return (value == null) ? accumulator : combineFn.addInput(accumulator, value);
    }

    if (args.size() == 1) {
      Object value = input.getValue(args.get(0));
      return (value == null) ? accumulator : combineFn.addInput(accumulator, value);
    }

    List<Object> argsValues = args.stream().map(input::getValue).collect(toList());

    if (argsValues.contains(null)) {
      return accumulator;
    }

    Schema argsSchema =
        args.stream().map(fieldIndex -> input.getSchema().getField(fieldIndex)).collect(toSchema());

    return combineFn.addInput(
        accumulator, Row.withSchema(argsSchema).addValues(argsValues).build());
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

    if (args.size() == 0) {
      Coder srcFieldCoder = RowCoder.coderForFieldType(sourceSchema.getField(0).getType());
      return combineFn.getAccumulatorCoder(registry, srcFieldCoder);
    }

    if (args.size() == 1) {
      int fieldIndex = args.size() == 0 ? 0 : args.get(0);
      Coder srcFieldCoder = RowCoder.coderForFieldType(sourceSchema.getField(fieldIndex).getType());
      return combineFn.getAccumulatorCoder(registry, srcFieldCoder);
    }

    Schema argsSchema =
        args.stream().map(fieldIndex -> sourceSchema.getField(fieldIndex)).collect(toSchema());

    return combineFn.getAccumulatorCoder(registry, RowCoder.of(argsSchema));
  }
}
