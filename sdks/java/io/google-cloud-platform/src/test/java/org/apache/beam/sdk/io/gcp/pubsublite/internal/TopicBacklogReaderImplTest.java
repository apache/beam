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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.example;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.TopicStatsClient;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;
import com.google.protobuf.Timestamp;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@SuppressWarnings("uninitialized")
@RunWith(JUnit4.class)
public final class TopicBacklogReaderImplTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock TopicStatsClient mockClient;

  private TopicBacklogReader reader;

  @Before
  public void setUp() {
    initMocks(this);
    this.reader =
        new TopicBacklogReaderImpl(mockClient, example(TopicPath.class), example(Partition.class));
  }

  @SuppressWarnings("incompatible")
  @Test
  public void computeMessageStats_failure() {
    when(mockClient.computeMessageStats(
            example(TopicPath.class),
            example(Partition.class),
            example(Offset.class),
            Offset.of(Long.MAX_VALUE)))
        .thenReturn(
            ApiFutures.immediateFailedFuture(new CheckedApiException(Code.UNAVAILABLE).underlying));

    ApiException e =
        assertThrows(ApiException.class, () -> reader.computeMessageStats(example(Offset.class)));
    assertEquals(Code.UNAVAILABLE, e.getStatusCode().getCode());
  }

  @Test
  public void computeMessageStats_validResponseCached() {
    Timestamp minEventTime = Timestamp.newBuilder().setSeconds(1000).setNanos(10).build();
    Timestamp minPublishTime = Timestamp.newBuilder().setSeconds(1001).setNanos(11).build();
    ComputeMessageStatsResponse response =
        ComputeMessageStatsResponse.newBuilder()
            .setMessageCount(10)
            .setMessageBytes(100)
            .setMinimumEventTime(minEventTime.toBuilder().setSeconds(1002).build())
            .setMinimumPublishTime(minPublishTime)
            .build();

    when(mockClient.computeMessageStats(
            example(TopicPath.class),
            example(Partition.class),
            example(Offset.class),
            Offset.of(Long.MAX_VALUE)))
        .thenReturn(ApiFutures.immediateFuture(response));

    assertEquals(reader.computeMessageStats(example(Offset.class)), response);
  }
}
