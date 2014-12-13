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

package com.google.cloud.dataflow.sdk.util;

import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utilities for working with CloudCounters.
 */
public class CloudCounterUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CloudCounterUtils.class);

  public static List<MetricUpdate> extractCounters(
      CounterSet counters, boolean delta) {
    synchronized (counters) {
      List<MetricUpdate> cloudCounters = new ArrayList<>(counters.size());
      for (Counter<?> counter : counters) {
        try {
          MetricUpdate cloudCounter = extractCounter(counter, delta);
          if (cloudCounter != null) {
            cloudCounters.add(cloudCounter);
          }
        } catch (IllegalArgumentException exn) {
          LOG.warn("Error extracting counter value: ", exn);
        }
      }
      return cloudCounters;
    }
  }

  public static MetricUpdate extractCounter(Counter<?> counter, boolean delta) {
    // TODO: Omit no-op counter updates, for counters whose
    // values haven't changed since the last time we sent them.
    synchronized (counter) {
      MetricStructuredName name = new MetricStructuredName();
      name.setName(counter.getName());
      MetricUpdate metricUpdate = new MetricUpdate()
          .setName(name)
          .setKind(counter.getKind().name())
          .setCumulative(!delta);
      switch (counter.getKind()) {
        case SUM:
        case MAX:
        case MIN:
        case AND:
        case OR:
          metricUpdate.setScalar(CloudObject.forKnownType(counter.getAggregate(delta)));
          break;
        case MEAN: {
          long countUpdate = counter.getCount(delta);
          if (countUpdate <= 0) {
            return null;
          }
          metricUpdate.setMeanSum(CloudObject.forKnownType(counter.getAggregate(delta)));
          metricUpdate.setMeanCount(CloudObject.forKnownType(countUpdate));
          break;
        }
        case SET: {
          Set<?> values = counter.getSet(delta);
          if (values.isEmpty()) {
            return null;
          }
          Set<Object> encodedSet = new HashSet(values.size());
          for (Object value : values) {
            encodedSet.add(CloudObject.forKnownType(value));
          }
          metricUpdate.setSet(encodedSet);
          break;
        }
        default:
          throw new IllegalArgumentException("unexpected kind of counter");
      }
      if (delta) {
        counter.resetDelta();
      }
      return metricUpdate;
    }
  }
}
