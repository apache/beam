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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.ExecutionContext;

/**
 * An {@code Aggregator<InputT>} enables monitoring of values of type {@code InputT},
 * to be combined across all bundles.
 *
 * <p>Aggregators are created by calling {@link DoFn#createAggregator DoFn.createAggregatorForDoFn},
 * typically from the {@link DoFn} constructor. Elements can be added to the
 * {@code Aggregator} by calling {@link Aggregator#addValue}.
 *
 * <p>Aggregators are visible in the monitoring UI, when the pipeline is run
 * using DataflowRunner or BlockingDataflowRunner, along with
 * their current value. Aggregators may not become visible until the system
 * begins executing the ParDo transform that created them and/or their initial
 * value is changed.
 *
 * <p>Example:
 * <pre> {@code
 * class MyDoFn extends DoFn<String, String> {
 *   private Aggregator<Integer, Integer> myAggregator;
 *
 *   public MyDoFn() {
 *     myAggregator = createAggregatorForDoFn("myAggregator", new Sum.SumIntegerFn());
 *   }
 *
 *   @Override
 *   public void processElement(ProcessContext c) {
 *     myAggregator.addValue(1);
 *   }
 * }
 * } </pre>
 *
 * @param <InputT> the type of input values
 * @param <OutputT> the type of output values
 */
public interface Aggregator<InputT, OutputT> {

  /**
   * Adds a new value into the Aggregator.
   */
  void addValue(InputT value);

  /**
   * Returns the name of the Aggregator.
   */
  String getName();

  /**
   * Returns the {@link CombineFn}, which combines input elements in the
   * aggregator.
   */
  CombineFn<InputT, ?, OutputT> getCombineFn();

  /**
   * A factory for creating aggregators.
   */
  interface AggregatorFactory {
    /**
     * Create an aggregator with the given {@code name} and {@link CombineFn}.
     *
     *  <p>This method is called to create an aggregator for a {@link DoFn}. It receives the class
     *  of the {@link DoFn} being executed and the context of the step it is being executed in.
     */
    <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
        Class<?> fnClass, ExecutionContext.StepContext stepContext,
        String aggregatorName, CombineFn<InputT, AccumT, OutputT> combine);
  }

  // TODO: Consider the following additional API conveniences:
  // - In addition to createAggregatorForDoFn(), consider adding getAggregator() to
  //   avoid the need to store the aggregator locally in a DoFn, i.e., create
  //   if not already present.
  // - Add a shortcut for the most common aggregator:
  //   c.createAggregatorForDoFn("name", new Sum.SumIntegerFn()).
}
