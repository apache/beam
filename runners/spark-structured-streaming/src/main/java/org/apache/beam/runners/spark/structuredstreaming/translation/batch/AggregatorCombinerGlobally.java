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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.util.ArrayList;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;

public class AggregatorCombinerGlobally<InputT, AccumT, OutputT>
    extends Aggregator<InputT, AccumT, OutputT> {

  Combine.CombineFn<InputT, AccumT, OutputT> combineFn;

  public AggregatorCombinerGlobally(Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
    this.combineFn = combineFn;
  }

  @Override
  public AccumT zero() {
    return combineFn.createAccumulator();
  }

  @Override
  public AccumT reduce(AccumT accumulator, InputT input) {
    return combineFn.addInput(accumulator, input);
  }

  @Override
  public AccumT merge(AccumT accumulator1, AccumT accumulator2) {
    ArrayList<AccumT> accumulators = new ArrayList<>();
    accumulators.add(accumulator1);
    accumulators.add(accumulator2);
    return combineFn.mergeAccumulators(accumulators);
  }

  @Override
  public OutputT finish(AccumT reduction) {
    return combineFn.extractOutput(reduction);
  }

  @Override
  public Encoder<AccumT> bufferEncoder() {
    //TODO replace with accumulatorCoder if possible
    return EncoderHelpers.genericEncoder();
  }

  @Override
  public Encoder<OutputT> outputEncoder() {
    //TODO replace with outputCoder if possible
    return EncoderHelpers.genericEncoder();
  }
}
