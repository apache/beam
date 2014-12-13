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

package com.google.cloud.dataflow.sdk.util.common;

/**
 * A metric (e.g., CPU usage) that can be reported by a worker.
 *
 * @param <T> the type of the metric's value
 */
public abstract class Metric<T> {
  String name;
  T value;

  public Metric(String name, T value) {
    this.name = name;
    this.value = value;
  }

  public String getName() { return name; }

  public T getValue() { return value; }

  /**
   * A double-valued Metric.
   */
  public static class DoubleMetric extends Metric<Double> {
    public DoubleMetric(String name, double value) {
      super(name, value);
    }
  }
}
