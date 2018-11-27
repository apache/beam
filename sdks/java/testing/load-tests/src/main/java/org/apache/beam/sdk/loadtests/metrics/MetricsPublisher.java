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
package org.apache.beam.sdk.loadtests.metrics;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;

/** Provides ways to publish metrics gathered during test invocation. */
public class MetricsPublisher {

  /**
   * This prints out metrics results to console. It will work only if metrics with appropriate
   * (conventional) names are present to be collected in {@link PipelineResult}
   *
   * <p>See {@link org.apache.beam.sdk.loadtests.GroupByKeyLoadTest} for hints on how to use it.
   */
  public static void toConsole(PipelineResult result, String namespace) {
    MetricsReader resultMetrics = new MetricsReader(result, namespace);

    long totalBytes = resultMetrics.getCounterMetric("totalBytes.count");
    long startTime = resultMetrics.getStartTimeMetric("runtime");
    long endTime = resultMetrics.getEndTimeMetric("runtime");

    System.out.println(String.format("Total bytes: %s", totalBytes));
    System.out.println(String.format("Total time (millis): %s", endTime - startTime));
  }
}
