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

import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/** This holds system metrics related constants used in Batch and Streaming. */
public class DataflowSystemMetrics {

  public static final MetricName THROTTLING_MSECS_METRIC_NAME =
      MetricName.named("dataflow-throttling-metrics", Metrics.THROTTLE_TIME_COUNTER_NAME);

  /** System counters populated by streaming dataflow workers. */
  public enum StreamingSystemCounterNames {
    WINDMILL_SHUFFLE_BYTES_READ("WindmillShuffleBytesRead"),
    WINDMILL_STATE_BYTES_READ("WindmillStateBytesRead"),
    WINDMILL_STATE_BYTES_WRITTEN("WindmillStateBytesWritten"),
    WINDMILL_MAX_WORK_ITEM_COMMIT_BYTES("WindmillMaxWorkItemCommitBytes"),
    JAVA_HARNESS_USED_MEMORY("dataflow_java_harness_used_memory"),
    JAVA_HARNESS_MAX_MEMORY("dataflow_java_harness_max_memory"),
    JAVA_HARNESS_RESTARTS("dataflow_java_harness_restarts"),
    TIME_AT_MAX_ACTIVE_THREADS("dataflow_time_at_max_active_threads"),
    ACTIVE_THREADS("dataflow_active_threads"),
    TOTAL_ALLOCATED_THREADS("dataflow_total_allocated_threads"),
    OUTSTANDING_BYTES("dataflow_outstanding_bytes"),
    MAX_OUTSTANDING_BYTES("dataflow_max_outstanding_bytes"),
    OUTSTANDING_BUNDLES("dataflow_outstanding_bundles"),
    MAX_OUTSTANDING_BUNDLES("dataflow_max_outstanding_bundles"),
    WINDMILL_QUOTA_THROTTLING("dataflow_streaming_engine_throttled_msecs"),
    MEMORY_THRASHING("dataflow_streaming_engine_user_worker_thrashing");

    private final String name;

    StreamingSystemCounterNames(String name) {
      this.name = name;
    }

    public CounterName counterName() {
      return CounterName.named(name);
    }
  }

  /** System counters populated by streaming dataflow worker for each stage. */
  public enum StreamingPerStageSystemCounterNames {

    /**
     * Total amount of time spent processing a stage, aggregated across all the concurrent tasks for
     * a stage.
     */
    TOTAL_PROCESSING_MSECS("dataflow_total_processing_msecs"),

    /**
     * Total amount of time spent processing a stage, aggregated across all the concurrent tasks for
     * a stage.
     */
    TIMER_PROCESSING_MSECS("dataflow_timer_processing_msecs"),

    /**
     * This is based on user updated metric "throttled-msecs", reported as part of system metrics so
     * that streaming autoscaler can access it.
     */
    THROTTLED_MSECS("dataflow_throttled_msecs");

    private final String namePrefix;

    StreamingPerStageSystemCounterNames(String namePrefix) {
      this.namePrefix = namePrefix;
    }

    public CounterName counterName(NameContext nameContext) {
      Preconditions.checkNotNull(nameContext.systemName());
      return CounterName.named(namePrefix + "-" + nameContext.systemName());
    }
  }
}
