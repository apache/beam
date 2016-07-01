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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.runners.AggregatorValues;
import org.apache.beam.sdk.transforms.Aggregator;

import com.google.common.base.MoreObjects;

import java.util.Map;

/**
 * An {@link AggregatorValues} implementation that is backed by an in-memory map.
 *
 * @param <T> the output type of the {@link Aggregator}
 */
public class MapAggregatorValues<T> extends AggregatorValues<T> {
  private final Map<String, T> stepValues;

  public MapAggregatorValues(Map<String, T> stepValues) {
    this.stepValues = stepValues;
  }

  @Override
  public Map<String, T> getValuesAtSteps() {
    return stepValues;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(MapAggregatorValues.class)
        .add("stepValues", stepValues)
        .toString();
  }
}
