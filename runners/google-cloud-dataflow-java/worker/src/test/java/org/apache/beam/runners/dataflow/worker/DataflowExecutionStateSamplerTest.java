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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
import org.joda.time.DateTimeUtils.MillisProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataflowExecutionStateSamplerTest {

  private MillisProvider clock;
  private DataflowExecutionStateSampler sampler;

  @Before
  public void setUp() {
    clock = mock(MillisProvider.class);
    sampler = DataflowExecutionStateSampler.newForTest(clock);
  }

  private final TestOperationContext.TestDataflowExecutionState step1act1 =
      new TestOperationContext.TestDataflowExecutionState(
          createNameContext("test-stage1"), "activity1");

  private NameContext createNameContext(String userName) {
    return NameContext.create("", "", "", userName);
  }

  @Test
  public void testAddTrackerRemoveTrackerActiveMessageMetadataGetsUpdated() {
    String workId = "work-item-id1";
    ActiveMessageMetadata testMetadata =
        ActiveMessageMetadata.create(step1act1.getStepName().userName(), Stopwatch.createStarted());
    DataflowExecutionStateTracker trackerMock = createMockTracker(workId);
    when(trackerMock.getActiveMessageMetadata()).thenReturn(Optional.of(testMetadata));

    sampler.addTracker(trackerMock);
    assertThat(sampler.getActiveMessageMetadataForWorkId(workId).get(), equalTo(testMetadata));

    sampler.removeTracker(trackerMock);
    Assert.assertFalse(sampler.getActiveMessageMetadataForWorkId(workId).isPresent());
  }

  @Test
  public void testRemoveTrackerCompletedProcessingTimesGetsUpdated() {
    String workId = "work-item-id1";
    Map<String, IntSummaryStatistics> testCompletedProcessingTimes = new HashMap<>();
    testCompletedProcessingTimes.put("some-step", new IntSummaryStatistics());
    DataflowExecutionStateTracker trackerMock = createMockTracker(workId);
    when(trackerMock.getProcessingTimesByStepCopy()).thenReturn(testCompletedProcessingTimes);

    sampler.addTracker(trackerMock);
    sampler.removeTracker(trackerMock);

    assertThat(
        sampler.getProcessingDistributionsForWorkId(workId), equalTo(testCompletedProcessingTimes));
  }

  @Test
  public void testGetCompletedProcessingTimesAndActiveMessageFromActiveTracker() {
    String workId = "work-item-id1";
    Map<String, IntSummaryStatistics> testCompletedProcessingTimes = new HashMap<>();
    IntSummaryStatistics testSummaryStats = new IntSummaryStatistics();
    testSummaryStats.accept(1);
    testSummaryStats.accept(3);
    testSummaryStats.accept(5);
    testCompletedProcessingTimes.put("some-step", testSummaryStats);
    ActiveMessageMetadata testMetadata =
        ActiveMessageMetadata.create(step1act1.getStepName().userName(), Stopwatch.createStarted());
    DataflowExecutionStateTracker trackerMock = createMockTracker(workId);
    when(trackerMock.getActiveMessageMetadata()).thenReturn(Optional.of(testMetadata));
    when(trackerMock.getProcessingTimesByStepCopy()).thenReturn(testCompletedProcessingTimes);

    sampler.addTracker(trackerMock);

    assertThat(sampler.getActiveMessageMetadataForWorkId(workId).get(), equalTo(testMetadata));
    assertThat(
        sampler.getProcessingDistributionsForWorkId(workId), equalTo(testCompletedProcessingTimes));
    // Repeated calls should not modify the result.
    assertThat(
        sampler.getProcessingDistributionsForWorkId(workId), equalTo(testCompletedProcessingTimes));
  }

  @Test
  public void testResetForWorkIdClearsMaps() {
    String workId1 = "work-item-id1";
    String workId2 = "work-item-id2";
    DataflowExecutionStateTracker tracker1Mock = createMockTracker(workId1);
    DataflowExecutionStateTracker tracker2Mock = createMockTracker(workId2);

    sampler.addTracker(tracker1Mock);
    sampler.addTracker(tracker2Mock);

    assertThat(
        sampler.getActiveMessageMetadataForWorkId(workId1),
        equalTo(tracker1Mock.getActiveMessageMetadata()));
    assertThat(
        sampler.getProcessingDistributionsForWorkId(workId1),
        equalTo(tracker1Mock.getProcessingTimesByStepCopy()));
    assertThat(
        sampler.getActiveMessageMetadataForWorkId(workId2),
        equalTo(tracker2Mock.getActiveMessageMetadata()));
    assertThat(
        sampler.getProcessingDistributionsForWorkId(workId2),
        equalTo(tracker2Mock.getProcessingTimesByStepCopy()));

    sampler.removeTracker(tracker1Mock);
    sampler.removeTracker(tracker2Mock);
    sampler.resetForWorkId(workId2);

    assertThat(
        sampler.getProcessingDistributionsForWorkId(workId1),
        equalTo(tracker1Mock.getProcessingTimesByStepCopy()));
    Assert.assertTrue(sampler.getProcessingDistributionsForWorkId(workId2).isEmpty());
  }

  private DataflowExecutionStateTracker createMockTracker(String workItemId) {
    DataflowExecutionStateTracker trackerMock = mock(DataflowExecutionStateTracker.class);
    when(trackerMock.getWorkItemId()).thenReturn(workItemId);
    return trackerMock;
  }
}
