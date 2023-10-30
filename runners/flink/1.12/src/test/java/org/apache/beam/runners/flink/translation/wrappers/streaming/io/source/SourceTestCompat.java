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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

public class SourceTestCompat {

  /** A MetricGroup implementation which records the registered gauge. */
  public static class TestMetricGroup
      extends UnregisteredMetricGroups.UnregisteredOperatorMetricGroup {
    public final Map<String, Gauge<?>> registeredGauge = new HashMap<>();
    public final Map<String, Counter> registeredCounter = new HashMap<>();
    public final Counter numRecordsInCounter = new SimpleCounter();

    @Override
    public <T, GaugeT extends Gauge<T>> GaugeT gauge(String name, GaugeT gauge) {
      registeredGauge.put(name, gauge);
      return gauge;
    }

    @Override
    public Counter counter(String name) {
      // The OperatorIOMetricsGroup will register some IO metrics in the constructor.
      // At that time, the construction of this class has not finihsed yet, so we
      // need to delegate the call to the parent class.
      if (registeredCounter != null) {
        return registeredCounter.computeIfAbsent(name, ignored -> super.counter(name));
      } else {
        return super.counter(name);
      }
    }

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
      return new OperatorIOMetricGroup(this) {
        @Override
        public Counter getNumRecordsInCounter() {
          return numRecordsInCounter;
        }
      };
    }
  }

  public interface ReaderOutputCompat<T> extends ReaderOutput<T> {
    void markActive();
  }

  public interface SourceOutputCompat<T> extends SourceOutput<T> {
    void markActive();
  }
}
