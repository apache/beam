/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.transforms;

/**
 * An {@code Aggregator} enables arbitrary monitoring in user code.
 *
 * <p> Aggregators are created by calling {@link DoFn.Context#createAggregator},
 * typically from {@link DoFn#startBundle}. Elements can be added to the
 * {@code Aggregator} by calling {@link Aggregator#addValue}.
 *
 * <p> Aggregators are visible in the monitoring UI, when the pipeline is run
 * using DataflowPipelineRunner or BlockingDataflowPipelineRunner, along with
 * their current value. Aggregators may not become visible until the system
 * begins executing the ParDo transform which created them and/or their initial
 * value is changed.
 *
 * <p> Example:
 * <pre> {@code
 * class MyDoFn extends DoFn<String, String> {
 *   private Aggregator<Integer> myAggregator;
 *
 *   {@literal @}Override
 *   public void startBundle(Context c) {
 *     myAggregator = c.createAggregator("myCounter", new Sum.SumIntegerFn());
 *   }
 *
 *   {@literal @}Override
 *   public void processElement(ProcessContext c) {
 *     myAggregator.addValue(1);
 *   }
 * }
 * } </pre>
 *
 * @param <VI> the type of input values
 */
public interface Aggregator<VI> {

  /**
   * Adds a new value into the Aggregator.
   */
  public void addValue(VI value);

  // TODO: Consider the following additional API conveniences:
  // - In addition to createAggregator(), consider adding getAggregator() to
  //   avoid the need to store the aggregator locally in a DoFn, i.e., create
  //   if not already present.
  // - Add a shortcut for the most common aggregator:
  //   c.createAggregator("name", new Sum.SumIntegerFn()).
}
