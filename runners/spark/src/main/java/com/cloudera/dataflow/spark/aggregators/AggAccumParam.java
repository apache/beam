/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark.aggregators;

import org.apache.spark.AccumulatorParam;

public class AggAccumParam implements AccumulatorParam<NamedAggregators> {
  @Override
  public NamedAggregators addAccumulator(NamedAggregators current, NamedAggregators added) {
    return current.merge(added);
  }

  @Override
  public NamedAggregators addInPlace(NamedAggregators current, NamedAggregators added) {
    return addAccumulator(current, added);
  }

  @Override
  public NamedAggregators zero(NamedAggregators initialValue) {
    return new NamedAggregators();
  }
}
