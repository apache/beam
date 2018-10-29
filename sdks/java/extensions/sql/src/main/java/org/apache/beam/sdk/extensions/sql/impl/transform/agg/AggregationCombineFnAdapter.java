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

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.sql.impl.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamBuiltinAggregations;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.AggregationArgsAdapter.ArgsAdapter;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.Pair;

/**
 * Wrapper {@link CombineFn} for aggregation function call.
 *
 * <p>Delegates to the actual aggregation {@link CombineFn}, either built-in, or UDAF.
 *
 * <p>Actual aggregation {@link CombineFn CombineFns} expect their specific arguments, not the full
 * input row. This class uses {@link ArgsAdapter arg adapters} to extract and map the call arguments
 * to the {@link CombineFn CombineFn's} inputs.
 */
public class AggregationCombineFnAdapter extends CombineFn<Row, Object, Object> {

  // Field for a function call
  private Schema.Field field;

  // Actual aggregation CombineFn
  private CombineFn combineFn;

  // Adapter to convert input Row to CombineFn's arguments
  private ArgsAdapter argsAdapter;

  /** {@link Schema.Field} with this function call. */
  public Schema.Field field() {
    return field;
  }

  private AggregationCombineFnAdapter(
      Schema.Field field, CombineFn combineFn, ArgsAdapter argsAdapter) {
    this.field = field;
    this.combineFn = combineFn;
    this.argsAdapter = argsAdapter;
  }

  /**
   * Creates an instance of {@link AggregationCombineFnAdapter}.
   *
   * @param callWithAlias Calcite's output, represents a function call paired with its field alias
   */
  public static AggregationCombineFnAdapter of(
      Pair<AggregateCall, String> callWithAlias, Schema inputSchema) {
    AggregateCall call = callWithAlias.getKey();
    Schema.Field field = CalciteUtils.toField(callWithAlias.getValue(), call.getType());
    String functionName = call.getAggregation().getName();

    return new AggregationCombineFnAdapter(
        field,
        createCombineFn(call, field, functionName),
        AggregationArgsAdapter.of(call.getArgList(), inputSchema));
  }

  /** Creates either a UDAF or a built-in {@link CombineFn}. */
  private static CombineFn<?, ?, ?> createCombineFn(
      AggregateCall call, Schema.Field field, String functionName) {
    if (call.getAggregation() instanceof SqlUserDefinedAggFunction) {
      return getUdafCombineFn(call);
    }

    return BeamBuiltinAggregations.create(functionName, field.getType().getTypeName());
  }

  private static CombineFn<?, ?, ?> getUdafCombineFn(AggregateCall call) {
    try {
      return ((UdafImpl) ((SqlUserDefinedAggFunction) call.getAggregation()).function)
          .getCombineFn();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Object createAccumulator() {
    return combineFn.createAccumulator();
  }

  /**
   * Calls the args adapter to extract the fields from the input row and pass them into the actual
   * {@link CombineFn}. E.g. input of a MAX(f) is not a full row, but just a number.
   *
   * <p>If argument is null, skip it and return the original accumulator. This is what SQL
   * aggregations are supposed to do.
   */
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

  /**
   * {@link CombineFn#getAccumulatorCoder} is supposed to use input {@link Coder coder} to infer the
   * {@link Coder coder} for the accumulator. Here we call the args adapter to get the input coder
   * for the delegate {@link CombineFn}.
   */
  @Override
  public Coder<Object> getAccumulatorCoder(CoderRegistry registry, Coder<Row> inputCoder)
      throws CannotProvideCoderException {

    return combineFn.getAccumulatorCoder(registry, argsAdapter.getArgsValuesCoder());
  }
}
