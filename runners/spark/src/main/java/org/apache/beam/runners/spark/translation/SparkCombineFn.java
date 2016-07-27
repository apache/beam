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

package org.apache.beam.runners.spark.translation;

import org.apache.beam.runners.spark.coders.EncoderHelpers;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import scala.Option;

/**
 * Spark runner implementation of {@link Combine.CombineFn}.
 *
 * Using {@link Option} to wrap AccumT in case the combine function returns null
 * for createAccumulator().
 * //TODO: remove usage of Option once possible - https://issues.apache.org/jira/browse/SPARK-15810
 */
class SparkCombineFn<InputT, AccumT, OutputT>
    extends Aggregator<InputT, Option<AccumT>, OutputT> {

  private final Combine.CombineFn<InputT, AccumT, OutputT> fn;

  SparkCombineFn(Combine.CombineFn<InputT, AccumT, OutputT> fn) {
    this.fn = fn;
  }

  @Override
  public Option<AccumT> zero() {
    AccumT accum = fn.createAccumulator();
    if (accum != null) {
      return Option.apply(accum);
    } else {
      return Option.empty();
    }
  }

  @Override
  public Option<AccumT> reduce(Option<AccumT> accumOpt, InputT in) {
    return Option.apply(fn.addInput(valueOrNull(accumOpt), in));
  }

  @Override
  public Option<AccumT> merge(Option<AccumT> accumOpt1, Option<AccumT> accumOpt2) {
    Collection<AccumT> accums = Collections.unmodifiableCollection(
        Arrays.asList(valueOrNull(accumOpt1), valueOrNull(accumOpt2)));
    return Option.apply(fn.mergeAccumulators(accums));
  }

  private static <T> T valueOrNull(Option<T> option) {
    return option.isEmpty() ? null : option.get();
  }

  @Override
  public OutputT finish(Option<AccumT> reduction) {
    return fn.extractOutput(reduction.get());
  }

  @Override
  public Encoder<Option<AccumT>> bufferEncoder() {
    return EncoderHelpers.encoder();
  }

  @Override
  public Encoder<OutputT> outputEncoder() {
    return EncoderHelpers.encoder();
  }
}
