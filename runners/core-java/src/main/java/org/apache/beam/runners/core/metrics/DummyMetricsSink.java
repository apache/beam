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

package org.apache.beam.runners.core.metrics;

import org.apache.beam.sdk.metrics.MetricQueryResults;

/**
 * This is the default Metrics Sink that just store in a static field the first counter (if it
 * exists) attempted value. This is usefull for tests.
 */
public class DummyMetricsSink implements MetricsSink<Long> {
  private static long counterValue;

  public static long getCounterValue(){
    return counterValue;
  }
  private static void setCounterValue(Long value){
    counterValue = value;
  }

  public static void clear(){
    counterValue = 0L;
  }

  @Override public void writeMetrics(MetricQueryResults metricQueryResults) throws Exception {
    Long metrics =
        metricQueryResults.counters().iterator().hasNext()
            ? metricQueryResults.counters().iterator().next().attempted()
            : 0L;
    setCounterValue(metrics);
  }
}
