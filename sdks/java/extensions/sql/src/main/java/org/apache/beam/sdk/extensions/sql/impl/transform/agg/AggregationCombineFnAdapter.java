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

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.impl.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
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

  private Schema.Field field;

  private List<Integer> args;

  private Combine.CombineFn combineFn;

  private Schema sourceSchema;

  private ArgsAdapter argsAdapter;

  public Schema.Field field() {
    return field;
  }

  public AggregationCombineFnAdapter(
      Schema.Field field,
      List<Integer> args,
      Combine.CombineFn combineFn,
      Schema sourceSchema,
      ArgsAdapter argsAdapter) {
    this.field = field;
    this.args = args;
    this.combineFn = combineFn;
    this.sourceSchema = sourceSchema;
    this.argsAdapter = argsAdapter;
  }

  public static AggregationCombineFnAdapter of(
      Pair<AggregateCall, String> callWithAlias, Schema inputSchema) {
    AggregateCall call = callWithAlias.getKey();
    Schema.Field field = CalciteUtils.toField(callWithAlias.getValue(), call.getType());
    String functionName = call.getAggregation().getName();

    return new AggregationCombineFnAdapter(
        field,
        call.getArgList(),
        createCombineFn(call, field, functionName),
        inputSchema,
        createArgsAdapter(call.getArgList(), inputSchema));
  }

  private static ArgsAdapter createArgsAdapter(List<Integer> argList, Schema inputSchema) {
    if (argList.size() == 0) {
      return new ZeroArgsAdapter(inputSchema);
    } else if (argList.size() == 1) {
      return new SingleArgAdapter(inputSchema, argList);
    } else {
      return new MultiArgsAdapter(inputSchema, argList);
    }
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
    Object argsValues = argsAdapter.getArgsValues(input);
    return (argsValues == null) ? accumulator : combineFn.addInput(accumulator, argsValues);
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

    return combineFn.getAccumulatorCoder(registry, argsAdapter.getArgsValuesCoder());
  }

  static class ZeroArgsAdapter implements ArgsAdapter {
    Schema sourceSchema;

    public ZeroArgsAdapter(Schema sourceSchema) {
      this.sourceSchema = sourceSchema;
    }

    @Nullable
    @Override
    public Object getArgsValues(Row input) {
      return input.getValue(0);
    }

    @Override
    public Coder getArgsValuesCoder() {
      return RowCoder.coderForFieldType(sourceSchema.getField(0).getType());
    }
  }

  static class SingleArgAdapter implements ArgsAdapter {
    Schema sourceSchema;
    List<Integer> argsIndicies;

    public SingleArgAdapter(Schema sourceSchema, List<Integer> argsIndicies) {
      this.sourceSchema = sourceSchema;
      this.argsIndicies = argsIndicies;
    }

    @Nullable
    @Override
    public Object getArgsValues(Row input) {
      return input.getValue(argsIndicies.get(0));
    }

    @Override
    public Coder getArgsValuesCoder() {
      int fieldIndex = argsIndicies.get(0);
      return RowCoder.coderForFieldType(sourceSchema.getField(fieldIndex).getType());
    }
  }

  static class MultiArgsAdapter implements ArgsAdapter {
    Schema sourceSchema;
    List<Integer> argsIndicies;

    public MultiArgsAdapter(Schema sourceSchema, List<Integer> argsIndicies) {
      this.sourceSchema = sourceSchema;
      this.argsIndicies = argsIndicies;
    }

    @Nullable
    @Override
    public Object getArgsValues(Row input) {

      List<Object> argsValues = argsIndicies.stream().map(input::getValue).collect(toList());

      if (argsValues.contains(null)) {
        return null;
      }

      Schema argsSchema =
          argsIndicies
              .stream()
              .map(fieldIndex -> input.getSchema().getField(fieldIndex))
              .collect(toSchema());

      return Row.withSchema(argsSchema).addValues(argsValues).build();
    }

    @Override
    public Coder getArgsValuesCoder() {
      Schema argsSchema =
          argsIndicies
              .stream()
              .map(fieldIndex -> sourceSchema.getField(fieldIndex))
              .collect(toSchema());

      return SchemaCoder.of(argsSchema);
    }
  }

  interface ArgsAdapter extends Serializable {
    @Nullable
    Object getArgsValues(Row input);

    Coder getArgsValuesCoder();
  }
}
