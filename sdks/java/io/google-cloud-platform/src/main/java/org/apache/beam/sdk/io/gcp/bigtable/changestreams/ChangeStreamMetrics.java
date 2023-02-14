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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

/** Class to aggregate metrics related functionality. */
@Internal
public class ChangeStreamMetrics implements Serializable {
  private static final long serialVersionUID = 7298901109362981596L;
  // ------------------------
  // Partition record metrics

  /**
   * Counter for the total number of partitions identified during the execution of the Connector.
   */
  public static final Counter LIST_PARTITIONS_COUNT =
      Metrics.counter(
          org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics.class,
          "list_partitions_count");

  /**
   * Increments the {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics#LIST_PARTITIONS_COUNT} by
   * 1 if the metric is enabled.
   */
  public void incListPartitionsCount() {
    inc(LIST_PARTITIONS_COUNT);
  }

  private void inc(Counter counter) {
    counter.inc();
  }
}
