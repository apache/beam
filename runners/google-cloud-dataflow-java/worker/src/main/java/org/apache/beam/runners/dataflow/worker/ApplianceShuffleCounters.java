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
package org.apache.beam.runners.dataflow.worker;

import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;

/**
 * ApplianceShuffleCounters implements functionality needed for importing counters from native
 * Appliance Shuffle code into Java.
 */
public final class ApplianceShuffleCounters {
  /** Factory fro creating counters. */
  private final CounterFactory counterFactory;

  /** Prefix string used for the counters. */
  private final String counterPrefix;

  /**
   * @param counterFactory factory that can be used to create/reuse counters
   * @param nameContext details about the operation these counters are for
   */
  public ApplianceShuffleCounters(
      CounterFactory counterFactory, NameContext nameContext, String datasetId) {
    this.counterFactory = counterFactory;
    this.counterPrefix =
        String.format("%s-%s-%s-", nameContext.stageName(), nameContext.systemName(), datasetId);
  }

  /**
   * Invoked to import a set of counter deltas. All the arrays passed in as parameters are required
   * to have the same length.
   *
   * @param counterNames names of counters to import
   * @param counterKinds kinds of counters to import ("sum", "max", or "min")
   * @param counterDeltas counter deltas to import
   */
  public void importCounters(String[] counterNames, String[] counterKinds, long[] counterDeltas) {
    final int length = counterNames.length;
    if (counterKinds.length != length || counterDeltas.length != length) {
      throw new AssertionError("array lengths do not match");
    }
    for (int i = 0; i < length; ++i) {
      final CounterName name = CounterName.named(counterPrefix + counterNames[i]);
      final String kind = counterKinds[i];
      final long delta = counterDeltas[i];
      switch (kind) {
        case "sum":
          counterFactory.longSum(name).addValue(delta);
          break;
        case "max":
          counterFactory.longMax(name).addValue(delta);
          break;
        case "min":
          counterFactory.longMin(name).addValue(delta);
          break;
        default:
          throw new IllegalArgumentException("unsupported counter kind: " + kind);
      }
    }
  }
}
