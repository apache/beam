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
package org.apache.beam.runners.samza.metrics;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.samza.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SamzaTransformMetricRegistry is a registry that maintains the metrics for each transform. It
 * maintains the average arrival time for each PCollection for a primitive transform.
 *
 * <p>For a non-data shuffling primitive transform, the average arrival time is calculated per
 * watermark, per PCollection {@link org.apache.beam.sdk.values.PValue} and updated in
 * avgArrivalTimeMap
 */
public class SamzaTransformMetricRegistry implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaTransformMetricRegistry.class);

  // TransformName -> PValue for pCollection -> Map<WatermarkId, AvgArrivalTime>
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>>>
      avgArrivalTimeMap;

  // Per Transform Metrics for each primitive transform
  private final SamzaTransformMetrics transformMetrics;

  public SamzaTransformMetricRegistry() {
    this.avgArrivalTimeMap = new ConcurrentHashMap<>();
    this.transformMetrics = new SamzaTransformMetrics();
  }

  public void register(String transformFullName, String pValue, Context ctx) {
    transformMetrics.register(transformFullName, ctx);
    // initialize the map for the transform
    avgArrivalTimeMap.putIfAbsent(transformFullName, new ConcurrentHashMap<>());
    avgArrivalTimeMap.get(transformFullName).putIfAbsent(pValue, new ConcurrentHashMap<>());
  }

  public SamzaTransformMetrics getTransformMetrics() {
    return transformMetrics;
  }

  public void updateArrivalTimeMap(String transformName, String pValue, long watermark, long avg) {
    if (avgArrivalTimeMap.get(transformName) != null
        && avgArrivalTimeMap.get(transformName).get(pValue) != null) {
      ConcurrentHashMap<Long, Long> avgArrivalTimeMapForPValue =
          avgArrivalTimeMap.get(transformName).get(pValue);
      // update the average arrival time for the latest watermark
      avgArrivalTimeMapForPValue.put(watermark, avg);
      // remove any stale entries which are lesser than the watermark
      // todo: check is this safe to do here input metric op may be ahead in watermark than output?
      // why not do it at emission time?
      avgArrivalTimeMapForPValue.entrySet().removeIf(entry -> entry.getKey() < watermark);
    }
  }

  // Checker framework bug: https://github.com/typetools/checker-framework/issues/979
  @SuppressWarnings("return")
  public void emitLatencyMetric(
      String transformName,
      List<String> inputs,
      List<String> outputs,
      Long watermark,
      String taskName) {
    final ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> avgArrivalTimeMapForTransform =
        avgArrivalTimeMap.get(transformName);

    if (avgArrivalTimeMapForTransform == null || inputs.isEmpty() || outputs.isEmpty()) {
      return;
    }

    // get the avg arrival times for all the input PValues
    final List<Long> inputPValuesAvgArrivalTimes =
        inputs.stream()
            .map(avgArrivalTimeMapForTransform::get)
            .map(map -> map == null ? null : map.remove(watermark))
            .filter(avgArrivalTime -> avgArrivalTime != null)
            .collect(Collectors.toList());

    // get the avg arrival times for all the output PValues
    final List<Long> outputPValuesAvgArrivalTimes =
        outputs.stream()
            .map(avgArrivalTimeMapForTransform::get)
            .map(map -> map == null ? null : map.remove(watermark))
            .filter(avgArrivalTime -> avgArrivalTime != null)
            .collect(Collectors.toList());

    if (inputPValuesAvgArrivalTimes.isEmpty() || outputPValuesAvgArrivalTimes.isEmpty()) {
      LOG.debug(
          "Failure to Emit Metric for Transform: {} inputArrivalTime: {} or outputArrivalTime: {} not found for Watermark: {} Task: {}",
          transformName,
          inputPValuesAvgArrivalTimes,
          inputPValuesAvgArrivalTimes,
          watermark,
          taskName);
      return;
    }

    final long startTime = Collections.min(inputPValuesAvgArrivalTimes);
    final long endTime = Collections.max(inputPValuesAvgArrivalTimes);
    final long latency = endTime - startTime;
    transformMetrics.getTransformLatencyMetric(transformName).update(latency);
    LOG.debug(
        "Success Emit Metric Transform: {} for watermark: {} for task: {}",
        transformName,
        watermark,
        taskName);
  }
}
