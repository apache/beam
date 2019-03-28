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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.MetricShortId;
import com.google.api.services.dataflow.model.NameAndKind;
import com.google.api.services.dataflow.model.ReportWorkItemStatusRequest;
import com.google.api.services.dataflow.model.ReportWorkItemStatusResponse;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import com.google.api.services.dataflow.model.WorkItemStatus;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link CounterShortIdCache}. */
@RunWith(JUnit4.class)
public class CounterShortIdCacheTest {
  @Rule public final ExpectedException thrown = ExpectedException.none();

  private CounterUpdate createMetricUpdateNameAndKind(final String name) {
    CounterUpdate metricUpdate = new CounterUpdate();
    NameAndKind nameAndKind = new NameAndKind();
    nameAndKind.setName(name);
    metricUpdate.setNameAndKind(nameAndKind);
    return metricUpdate;
  }

  private List<WorkItemStatus> createWorkStatusNameAndKind(String[]... counterNames) {
    List<WorkItemStatus> statuses = new ArrayList<>();
    for (String[] names : counterNames) {
      WorkItemStatus status = new WorkItemStatus();
      List<CounterUpdate> counterList = new ArrayList<>();
      for (String name : names) {
        counterList.add(createMetricUpdateNameAndKind(name));
      }
      status.setCounterUpdates(counterList);
      statuses.add(status);
    }
    return statuses;
  }

  private CounterUpdate createMetricUpdateStructuredName(final String name) {
    CounterUpdate metricUpdate = new CounterUpdate();
    metricUpdate.setStructuredNameAndMetadata(
        new CounterStructuredNameAndMetadata().setName(new CounterStructuredName().setName(name)));
    return metricUpdate;
  }

  private List<WorkItemStatus> createWorkStatusStructuredName(String[]... counterNames) {
    List<WorkItemStatus> statuses = new ArrayList<>();
    for (String[] names : counterNames) {
      WorkItemStatus status = new WorkItemStatus();
      List<CounterUpdate> counterList = new ArrayList<>();
      for (String name : names) {
        counterList.add(createMetricUpdateStructuredName(name));
      }
      status.setCounterUpdates(counterList);
      statuses.add(status);
    }
    return statuses;
  }

  private MetricShortId createMetricShortId(int index, Long shortId) {
    MetricShortId id = new MetricShortId();
    id.setMetricIndex(index);
    id.setShortId(shortId);
    return id;
  }

  private List<WorkItemServiceState> createWorkServiceState(Long[]... counterIds) {
    List<WorkItemServiceState> states = new ArrayList<>();
    for (Long[] ids : counterIds) {
      WorkItemServiceState state = new WorkItemServiceState();
      List<MetricShortId> shortIds = new ArrayList<>();
      for (int i = 0; i < ids.length; i++) {
        shortIds.add(createMetricShortId(i, ids[i]));
      }
      state.setMetricShortId(shortIds);
      states.add(state);
    }
    return states;
  }

  private List<WorkItemServiceState> createWorkServiceState(MetricShortId[]... counterIds) {
    List<WorkItemServiceState> states = new ArrayList<>();
    for (MetricShortId[] ids : counterIds) {
      WorkItemServiceState state = new WorkItemServiceState();
      List<MetricShortId> shortIds = new ArrayList<>();
      for (int i = 0; i < ids.length; i++) {
        shortIds.add(ids[i]);
      }
      state.setMetricShortId(shortIds);
      states.add(state);
    }
    return states;
  }

  private void checkStatusAndShortIds(final WorkItemStatus status, Long... shortIds) {
    List<CounterUpdate> updates = status.getCounterUpdates();
    assertTrue(updates.size() == shortIds.length);
    for (int i = 0; i < updates.size(); i++) {
      assertNull(status.getCounterUpdates().get(i).getNameAndKind());
      assertTrue(status.getCounterUpdates().get(i).getShortId().longValue() == shortIds[i]);
    }
  }

  @Test
  public void testCacheNameAndKind() {
    CounterShortIdCache shortIdCache = new CounterShortIdCache();
    ReportWorkItemStatusRequest request = new ReportWorkItemStatusRequest();
    ReportWorkItemStatusResponse reply = new ReportWorkItemStatusResponse();

    // setup mock counters, three work statuses, one with two counters, one with one, one with none
    request.setWorkItemStatuses(
        createWorkStatusNameAndKind(
            new String[] {"counter", "counter1"}, new String[] {}, new String[] {"counter2"}));

    reply.setWorkItemServiceStates(
        createWorkServiceState(new Long[] {1000L, 1001L}, new Long[] {}, new Long[] {1002L}));

    // Verify the empty case
    WorkItemStatus status1 = request.getWorkItemStatuses().get(0);
    WorkItemStatus status2 = request.getWorkItemStatuses().get(1);
    WorkItemStatus status3 = request.getWorkItemStatuses().get(2);
    shortIdCache.shortenIdsIfAvailable(status1.getCounterUpdates());
    for (CounterUpdate update : status1.getCounterUpdates()) {
      assertNull(update.getShortId());
    }

    // Add the shortIds
    shortIdCache.storeNewShortIds(request, reply);
    shortIdCache.shortenIdsIfAvailable(status1.getCounterUpdates());
    shortIdCache.shortenIdsIfAvailable(status2.getCounterUpdates());
    shortIdCache.shortenIdsIfAvailable(status3.getCounterUpdates());
    checkStatusAndShortIds(status1, 1000L, 1001L);
    checkStatusAndShortIds(status2);
    checkStatusAndShortIds(status3, 1002L);
  }

  // This should not crash
  @Test
  public void testNullUpdates() {
    CounterShortIdCache shortIdCache = new CounterShortIdCache();
    shortIdCache.shortenIdsIfAvailable(null);
  }

  @Test
  public void testCacheStructuredName() {
    CounterShortIdCache shortIdCache = new CounterShortIdCache();
    ReportWorkItemStatusRequest request = new ReportWorkItemStatusRequest();
    ReportWorkItemStatusResponse reply = new ReportWorkItemStatusResponse();

    // setup mock counters, three work statuses, one with two counters, one with one, one with none
    request.setWorkItemStatuses(
        createWorkStatusStructuredName(
            new String[] {"counter", "counter1"}, new String[] {}, new String[] {"counter2"}));

    reply.setWorkItemServiceStates(
        createWorkServiceState(new Long[] {1000L, 1001L}, new Long[] {}, new Long[] {1002L}));

    // Verify the empty case
    WorkItemStatus status1 = request.getWorkItemStatuses().get(0);
    WorkItemStatus status2 = request.getWorkItemStatuses().get(1);
    WorkItemStatus status3 = request.getWorkItemStatuses().get(2);
    shortIdCache.shortenIdsIfAvailable(status1.getCounterUpdates());
    for (CounterUpdate update : status1.getCounterUpdates()) {
      assertNull(update.getShortId());
    }

    // Add the shortIds
    shortIdCache.storeNewShortIds(request, reply);
    shortIdCache.shortenIdsIfAvailable(status1.getCounterUpdates());
    shortIdCache.shortenIdsIfAvailable(status2.getCounterUpdates());
    shortIdCache.shortenIdsIfAvailable(status3.getCounterUpdates());
    checkStatusAndShortIds(status1, 1000L, 1001L);
    checkStatusAndShortIds(status2);
    checkStatusAndShortIds(status3, 1002L);
  }

  @Test
  public void testValidateNumberStatusesAndStates() {
    CounterShortIdCache cache = new CounterShortIdCache();
    ReportWorkItemStatusRequest request = new ReportWorkItemStatusRequest();
    ReportWorkItemStatusResponse reply = new ReportWorkItemStatusResponse();

    request.setWorkItemStatuses(
        createWorkStatusNameAndKind(new String[] {"counter"}, new String[] {"counter2"}));
    reply.setWorkItemServiceStates(
        createWorkServiceState(new MetricShortId[] {createMetricShortId(0, 1000L)}));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("RequestWorkItemStatus request and response are unbalanced");
    cache.storeNewShortIds(request, reply);
  }

  @Test
  public void testValidateShortIdsButNoUpdate() {
    CounterShortIdCache cache = new CounterShortIdCache();
    ReportWorkItemStatusRequest request = new ReportWorkItemStatusRequest();
    ReportWorkItemStatusResponse reply = new ReportWorkItemStatusResponse();

    request.setWorkItemStatuses(Arrays.asList(new WorkItemStatus()));
    reply.setWorkItemServiceStates(createWorkServiceState(new Long[] {1000L}));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Response has shortids but no corresponding CounterUpdate");
    cache.storeNewShortIds(request, reply);
  }

  @Test
  public void testValidateAggregateIndexOutOfRange() {
    CounterShortIdCache cache = new CounterShortIdCache();
    ReportWorkItemStatusRequest request = new ReportWorkItemStatusRequest();
    ReportWorkItemStatusResponse reply = new ReportWorkItemStatusResponse();

    request.setWorkItemStatuses(createWorkStatusNameAndKind(new String[] {"counter"}));
    reply.setWorkItemServiceStates(
        createWorkServiceState(new MetricShortId[] {createMetricShortId(1000, 1000L)}));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Received aggregate index outside range of sent update");
    cache.storeNewShortIds(request, reply);
  }
}
