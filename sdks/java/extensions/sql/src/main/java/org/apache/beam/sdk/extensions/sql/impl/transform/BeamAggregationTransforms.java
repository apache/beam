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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

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

  /** An adaptor class to invoke Calcite UDAF instances in Beam {@code CombineFn}. */
  public static class MultiAggregationCombineFn extends CombineFn<Row, List<Object>, Row> {
    private List<AggregationCombineFnAdapter> aggCombineFns;
    private Schema finalSchema;

    public MultiAggregationCombineFn(List<AggregationCombineFnAdapter> aggCombineFns) {
      this.aggCombineFns = aggCombineFns;
      finalSchema =
          this.aggCombineFns.stream().map(AggregationCombineFnAdapter::field).collect(toSchema());
    }

    @Override
    public List<Object> createAccumulator() {
      return aggCombineFns
          .stream()
          .map(AggregationCombineFnAdapter::createAccumulator)
          .collect(Collectors.toList());
    }

    @Override
    public List<Object> addInput(List<Object> accumulators, Row input) {
      List<Object> deltaAcc = new ArrayList<>();

      for (int idx = 0; idx < aggCombineFns.size(); ++idx) {
        AggregationCombineFnAdapter aggregator = aggCombineFns.get(idx);
        Object aggregatorAccumulator = accumulators.get(idx);

        Object newAccumulator = aggregator.addInput(aggregatorAccumulator, input);
        deltaAcc.add(newAccumulator);
      }
      return deltaAcc;
    }

    @Override
    public List<Object> mergeAccumulators(Iterable<List<Object>> accumulators) {
      List<Object> deltaAcc = new ArrayList<>();
      for (int idx = 0; idx < aggCombineFns.size(); ++idx) {
        List accs = new ArrayList<>();
        for (List<Object> accumulator : accumulators) {
          accs.add(accumulator.get(idx));
        }
        deltaAcc.add(aggCombineFns.get(idx).mergeAccumulators(accs));
      }
      return deltaAcc;
    }

    @Override
    public Row extractOutput(List<Object> accumulator) {
      return IntStream.range(0, aggCombineFns.size())
          .mapToObj(idx -> aggCombineFns.get(idx).extractOutput(accumulator.get(idx)))
          .collect(toRow(finalSchema));
    }

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
  }

  /** Coder for accumulators. */
  public static class AggregationAccumulatorCoder extends CustomCoder<List<Object>> {
    private VarIntCoder sizeCoder = VarIntCoder.of();
    private List<Coder> elementCoders;

    public AggregationAccumulatorCoder(List<Coder> elementCoders) {
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
