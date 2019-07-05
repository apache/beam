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
package org.apache.beam.runners.spark.structuredstreaming.aggregators;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For resilience, {@link AccumulatorV2 Accumulators} are required to be wrapped in a Singleton.
 *
 * @see <a
 *     href="https://spark.apache.org/docs/latest/api/java/org/apache/spark/util/AccumulatorV2.html">accumulatorsV2</a>
 */
public class AggregatorsAccumulator {
  private static final Logger LOG = LoggerFactory.getLogger(AggregatorsAccumulator.class);

  private static final String ACCUMULATOR_NAME = "Beam.Aggregators";

  private static volatile NamedAggregatorsAccumulator instance = null;

  /** Init aggregators accumulator if it has not been initiated. This method is idempotent. */
  public static void init(JavaSparkContext jsc) {
    if (instance == null) {
      synchronized (AggregatorsAccumulator.class) {
        if (instance == null) {
          NamedAggregators namedAggregators = new NamedAggregators();
          NamedAggregatorsAccumulator accumulator =
              new NamedAggregatorsAccumulator(namedAggregators);
          jsc.sc().register(accumulator, ACCUMULATOR_NAME);

          instance = accumulator;
        }
      }
      LOG.info("Instantiated aggregators accumulator: " + instance.value());
    }
  }

  public static NamedAggregatorsAccumulator getInstance() {
    if (instance == null) {
      throw new IllegalStateException("Aggregrators accumulator has not been instantiated");
    } else {
      return instance;
    }
  }

  @VisibleForTesting
  public static void clear() {
    synchronized (AggregatorsAccumulator.class) {
      instance = null;
    }
  }
}
