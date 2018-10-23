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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.AggregationCombineFnAdapter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.Row;

/**
 * Wrapper for multiple aggregations.
 *
 * <p>Maintains the accumulators and output schema. Delegates the aggregation to the combiners for
 * each separate aggregation call.
 *
 * <p>Output schema is the schema corresponding to the list of all aggregation calls.
 */
public class MultipleAggregationsFn extends CombineFn<Row, List<Object>, Row> {
  private List<AggregationCombineFnAdapter> aggCombineFns;
  private Schema outputSchema;

  /**
   * Returns an instance of {@link MultipleAggregationsFn}.
   *
   * @param aggCombineFns is the list of aggregation {@link CombineFn CombineFns} that perform the
   *     actual aggregations.
   */
  public static MultipleAggregationsFn combineFns(List<AggregationCombineFnAdapter> aggCombineFns) {
    return new MultipleAggregationsFn(aggCombineFns);
  }

  private MultipleAggregationsFn(List<AggregationCombineFnAdapter> aggCombineFns) {
    this.aggCombineFns = aggCombineFns;
    outputSchema =
        this.aggCombineFns.stream().map(AggregationCombineFnAdapter::field).collect(toSchema());
  }

  /**
   * Accumulator for this {@link CombineFn} is a list of accumulators of the underlying delegate
   * {@link CombineFn CombineFns}.
   */
  @Override
  public List<Object> createAccumulator() {
    return aggCombineFns
        .stream()
        .map(AggregationCombineFnAdapter::createAccumulator)
        .collect(Collectors.toList());
  }

  /** For each delegate {@link CombineFn} we use the corresponding accumulator from the list. */
  @Override
  public List<Object> addInput(List<Object> accumulators, Row input) {
    List<Object> newAcc = new ArrayList<>();

    for (int idx = 0; idx < aggCombineFns.size(); ++idx) {
      AggregationCombineFnAdapter aggregator = aggCombineFns.get(idx);
      Object aggregatorAccumulator = accumulators.get(idx);

      Object newAccumulator = aggregator.addInput(aggregatorAccumulator, input);
      newAcc.add(newAccumulator);
    }
    return newAcc;
  }

  /**
   * Collect all accumulators for the corresponding {@link CombineFn} into a list, and pass the list
   * for merging to the delegate.
   */
  @Override
  public List<Object> mergeAccumulators(Iterable<List<Object>> accumulators) {
    List<Object> newAcc = new ArrayList<>();
    for (int idx = 0; idx < aggCombineFns.size(); ++idx) {
      List accs = new ArrayList<>();
      for (List<Object> accumulator : accumulators) {
        accs.add(accumulator.get(idx));
      }
      newAcc.add(aggCombineFns.get(idx).mergeAccumulators(accs));
    }
    return newAcc;
  }

  /**
   * Just extract all outputs from the delegate {@link CombineFn CombineFns} and assemble them into
   * a row.
   */
  @Override
  public Row extractOutput(List<Object> accumulator) {
    return IntStream.range(0, aggCombineFns.size())
        .mapToObj(idx -> aggCombineFns.get(idx).extractOutput(accumulator.get(idx)))
        .collect(toRow(outputSchema));
  }

  /**
   * Accumulator coder is a special {@link AggregationAccumulatorCoder coder} that encodes a list of
   * all accumulators using accumulator coders from their {@link CombineFn CombineFns}.
   */
  @Override
  public Coder<List<Object>> getAccumulatorCoder(CoderRegistry registry, Coder<Row> inputCoder)
      throws CannotProvideCoderException {
    // TODO: Doing this here is wrong.
    registry.registerCoderForClass(BigDecimal.class, BigDecimalCoder.of());
    List<Coder> aggAccuCoderList = new ArrayList<>();

    for (AggregationCombineFnAdapter aggCombineFn : aggCombineFns) {
      aggAccuCoderList.add(aggCombineFn.getAccumulatorCoder(registry, inputCoder));
    }

    return new AggregationAccumulatorCoder(aggAccuCoderList);
  }

  /**
   * Coder for accumulators.
   *
   * <p>Takes in a list of accumulator coders, delegates encoding/decoding to them.
   */
  static class AggregationAccumulatorCoder extends CustomCoder<List<Object>> {
    private VarIntCoder sizeCoder = VarIntCoder.of();
    private List<Coder> elementCoders;

    AggregationAccumulatorCoder(List<Coder> elementCoders) {
      this.elementCoders = elementCoders;
    }

    @Override
    public void encode(List<Object> value, OutputStream outStream) throws IOException {
      sizeCoder.encode(value.size(), outStream);
      for (int idx = 0; idx < value.size(); ++idx) {
        elementCoders.get(idx).encode(value.get(idx), outStream);
      }
    }

    @Override
    public List<Object> decode(InputStream inStream) throws CoderException, IOException {
      List<Object> accu = new ArrayList<>();
      int size = sizeCoder.decode(inStream);
      for (int idx = 0; idx < size; ++idx) {
        accu.add(elementCoders.get(idx).decode(inStream));
      }
      return accu;
    }
  }
}
