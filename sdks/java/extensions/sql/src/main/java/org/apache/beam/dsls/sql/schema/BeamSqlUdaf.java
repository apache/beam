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
package org.apache.beam.dsls.sql.schema;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

/**
 * abstract class of aggregation functions in Beam SQL.
 *
 * <p>There're several constrains for a UDAF:<br>
 * 1. A constructor with an empty argument list is required;<br>
 * 2. The type of {@code InputT} and {@code OutputT} can only be Interger/Long/Short/Byte/Double
 * /Float/Date/BigDecimal, mapping as SQL type INTEGER/BIGINT/SMALLINT/TINYINE/DOUBLE/FLOAT
 * /TIMESTAMP/DECIMAL;<br>
 * 3. Keep intermediate data in {@code AccumT}, and do not rely on elements in class;<br>
 */
public abstract class BeamSqlUdaf<InputT, AccumT, OutputT> implements Serializable {
  public BeamSqlUdaf(){}

  /**
   * create an initial aggregation object, equals to {@link CombineFn#createAccumulator()}.
   */
  public abstract AccumT init();

  /**
   * add an input value, equals to {@link CombineFn#addInput(Object, Object)}.
   */
  public abstract AccumT add(AccumT accumulator, InputT input);

  /**
   * merge aggregation objects from parallel tasks, equals to
   *  {@link CombineFn#mergeAccumulators(Iterable)}.
   */
  public abstract AccumT merge(Iterable<AccumT> accumulators);

  /**
   * extract output value from aggregation object, equals to
   * {@link CombineFn#extractOutput(Object)}.
   */
  public abstract OutputT result(AccumT accumulator);

  /**
   * get the coder for AccumT which stores the intermediate result.
   * By default it's fetched from {@link CoderRegistry}.
   */
  public Coder<AccumT> getAccumulatorCoder(CoderRegistry registry)
      throws CannotProvideCoderException {
    return registry.getCoder(
        (Class<AccumT>) ((ParameterizedType) getClass()
        .getGenericSuperclass()).getActualTypeArguments()[1]);
  }
}
