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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.MetricShortId;
import com.google.api.services.dataflow.model.ReportWorkItemStatusRequest;
import com.google.api.services.dataflow.model.ReportWorkItemStatusResponse;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import com.google.api.services.dataflow.model.WorkItemStatus;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapping from counter names to short IDs.
 *
 * <p>This cache is non-evicting, and lives for the lifetime of the worker harness. This behavior is
 * fine because the total number of unique counters is expected to be small and limited by the
 * backend.
 */
public class CounterShortIdCache {
  private static final Logger LOG = LoggerFactory.getLogger(CounterShortIdCache.class);
  private Cache cache = new Cache();

  /**
   * Add any new short ids received to the table. The outgoing request will have the full counter
   * updates, and the incoming responses have the associated short ids. By matching up short ids
   * with the counters in order we can build a mapping of name -> short_id for future use.
   */
  public void storeNewShortIds(
      final ReportWorkItemStatusRequest request, final ReportWorkItemStatusResponse reply) {
    checkArgument(
        request.getWorkItemStatuses() != null
            && reply.getWorkItemServiceStates() != null
            && request.getWorkItemStatuses().size() == reply.getWorkItemServiceStates().size(),
        "RequestWorkItemStatus request and response are unbalanced, status: %s, states: %s",
        request.getWorkItemStatuses(),
        reply.getWorkItemServiceStates());

    for (int i = 0; i < request.getWorkItemStatuses().size(); i++) {
      WorkItemServiceState state = reply.getWorkItemServiceStates().get(i);
      WorkItemStatus status = request.getWorkItemStatuses().get(i);
      if (state.getMetricShortId() == null) {
        continue;
      }
      checkArgument(
          status.getCounterUpdates() != null,
          "Response has shortids but no corresponding CounterUpdate");
      for (MetricShortId shortIdMsg : state.getMetricShortId()) {
        int metricIndex = MoreObjects.firstNonNull(shortIdMsg.getMetricIndex(), 0);
        checkArgument(
            metricIndex < status.getCounterUpdates().size(),
            "Received aggregate index outside range of sent update %s >= %s",
            shortIdMsg.getMetricIndex(),
            status.getCounterUpdates().size());

        CounterUpdate update = status.getCounterUpdates().get(metricIndex);
        cache.insert(update, checkNotNull(shortIdMsg.getShortId(), "Shortid should be non-null"));
      }
    }
  }

  /**
   * If any aggregates match a short id in the table, replace their name and type with the short id.
   */
  public void shortenIdsIfAvailable(java.util.@Nullable List<CounterUpdate> counters) {
    if (counters == null) {
      return;
    }
    for (CounterUpdate update : counters) {
      cache.shortenIdsIfAvailable(update);
    }
  }

  private static class Cache<K> {
    private final ConcurrentHashMap<String, Long> unstructuredShortIdMap =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<CounterStructuredName, Long> structuredShortIdMap =
        new ConcurrentHashMap<>();

    public void shortenIdsIfAvailable(CounterUpdate update) {
      Long shortId;
      if (update.getNameAndKind() != null) {
        String name =
            checkNotNull(
                update.getNameAndKind().getName(), "Counter update name should be non-null");
        shortId = unstructuredShortIdMap.get(name);
      } else if (update.getStructuredNameAndMetadata() != null) {
        CounterStructuredName name =
            checkNotNull(
                update.getStructuredNameAndMetadata().getName(),
                "Counter update structured-name should be non-null");
        shortId = structuredShortIdMap.get(name);
      } else {
        throw new IllegalArgumentException(
            "CounterUpdate should have nameAndKind or structuredNameAndmetadata");
      }

      if (shortId != null) {
        update.setNameAndKind(null);
        update.setStructuredNameAndMetadata(null);
        update.setShortId(shortId);
      }
    }

    public void insert(CounterUpdate update, long shortId) {
      Long oldValue;
      if (update.getNameAndKind() != null) {
        String name =
            checkNotNull(
                update.getNameAndKind().getName(), "Counter update name should be non-null");
        oldValue = unstructuredShortIdMap.putIfAbsent(name, shortId);
      } else if (update.getStructuredNameAndMetadata() != null) {
        CounterStructuredName name =
            checkNotNull(
                update.getStructuredNameAndMetadata().getName(),
                "Counter update structured-name should be non-null");
        oldValue = structuredShortIdMap.putIfAbsent(name, shortId);
      } else {
        throw new IllegalArgumentException(
            "CounterUpdate should have nameAndKind or structuredNameAndmetadata");
      }

      checkArgument(
          oldValue == null || oldValue.equals(shortId),
          "Received counter %s with incompatible short IDs. %s first ID and then %s",
          update,
          oldValue,
          shortId);
    }
  }
}
