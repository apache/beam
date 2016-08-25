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

package org.apache.beam.runners.spark.aggregators;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * For resilience, {@link Accumulator}s are required to be wrapped in a Singleton.
 * @see <a href="https://spark.apache.org/docs/1.6.2/streaming-programming-guide.html#accumulators-and-broadcast-variables">accumulators</a>
 */
public class AccumulatorSingleton {

  private static volatile Accumulator<NamedAggregators> instance = null;

  public static Accumulator<NamedAggregators> getInstance(JavaSparkContext jsc) {
    if (instance == null) {
      synchronized (AccumulatorSingleton.class) {
        if (instance == null) {
          //TODO: currently when recovering from checkpoint, Spark does not recover the
          // last known Accumulator value. The SparkRunner should be able to persist and recover
          // the NamedAggregators in order to recover Aggregators as well.
          instance = jsc.sc().accumulator(new NamedAggregators(), new AggAccumParam());
        }
      }
    }
    return instance;
  }

  @VisibleForTesting
  public static void clear() {
    synchronized (AccumulatorSingleton.class) {
      instance = null;
    }
  }
}
