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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamShutdownException;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GrpcGetDataStreamRequestsTest {

  @Test
  public void testQueuedRequest_globalRequestsFirstComparator() {
    List<GrpcGetDataStreamRequests.QueuedRequest> requests = new ArrayList<>();
    Windmill.KeyedGetDataRequest keyedGetDataRequest1 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(ByteString.EMPTY)
            .setCacheToken(1L)
            .setShardingKey(1L)
            .setWorkToken(1L)
            .setMaxBytes(Long.MAX_VALUE)
            .build();
    requests.add(
        GrpcGetDataStreamRequests.QueuedRequest.forComputation(
            1, "computation1", keyedGetDataRequest1));

    Windmill.KeyedGetDataRequest keyedGetDataRequest2 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(ByteString.EMPTY)
            .setCacheToken(2L)
            .setShardingKey(2L)
            .setWorkToken(2L)
            .setMaxBytes(Long.MAX_VALUE)
            .build();
    requests.add(
        GrpcGetDataStreamRequests.QueuedRequest.forComputation(
            2, "computation2", keyedGetDataRequest2));

    Windmill.GlobalDataRequest globalDataRequest =
        Windmill.GlobalDataRequest.newBuilder()
            .setDataId(
                Windmill.GlobalDataId.newBuilder()
                    .setTag("globalData")
                    .setVersion(ByteString.EMPTY)
                    .build())
            .setComputationId("computation1")
            .build();
    requests.add(GrpcGetDataStreamRequests.QueuedRequest.global(3, globalDataRequest));

    requests.sort(GrpcGetDataStreamRequests.QueuedRequest.globalRequestsFirst());

    // First one should be the global request.
    assertTrue(requests.get(0).getDataRequest().isGlobal());
  }

  @Test
  public void testQueuedBatch_asGetDataRequest() {
    GrpcGetDataStreamRequests.QueuedBatch queuedBatch = new GrpcGetDataStreamRequests.QueuedBatch();

    Windmill.KeyedGetDataRequest keyedGetDataRequest1 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(ByteString.EMPTY)
            .setCacheToken(1L)
            .setShardingKey(1L)
            .setWorkToken(1L)
            .setMaxBytes(Long.MAX_VALUE)
            .build();
    queuedBatch.addRequest(
        GrpcGetDataStreamRequests.QueuedRequest.forComputation(
            1, "computation1", keyedGetDataRequest1));

    Windmill.KeyedGetDataRequest keyedGetDataRequest2 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(ByteString.EMPTY)
            .setCacheToken(2L)
            .setShardingKey(2L)
            .setWorkToken(2L)
            .setMaxBytes(Long.MAX_VALUE)
            .build();
    queuedBatch.addRequest(
        GrpcGetDataStreamRequests.QueuedRequest.forComputation(
            2, "computation2", keyedGetDataRequest2));

    Windmill.GlobalDataRequest globalDataRequest =
        Windmill.GlobalDataRequest.newBuilder()
            .setDataId(
                Windmill.GlobalDataId.newBuilder()
                    .setTag("globalData")
                    .setVersion(ByteString.EMPTY)
                    .build())
            .setComputationId("computation1")
            .build();
    queuedBatch.addRequest(GrpcGetDataStreamRequests.QueuedRequest.global(3, globalDataRequest));

    Windmill.StreamingGetDataRequest getDataRequest = queuedBatch.asGetDataRequest();

    assertThat(getDataRequest.getRequestIdCount()).isEqualTo(3);
    assertThat(getDataRequest.getGlobalDataRequestList()).containsExactly(globalDataRequest);
    assertThat(getDataRequest.getStateRequestList())
        .containsExactly(
            Windmill.ComputationGetDataRequest.newBuilder()
                .setComputationId("computation1")
                .addRequests(keyedGetDataRequest1)
                .build(),
            Windmill.ComputationGetDataRequest.newBuilder()
                .setComputationId("computation2")
                .addRequests(keyedGetDataRequest2)
                .build());
  }

  @Test
  public void testQueuedBatch_notifyFailed_throwsWindmillStreamShutdownExceptionOnWaiters() {
    GrpcGetDataStreamRequests.QueuedBatch queuedBatch = new GrpcGetDataStreamRequests.QueuedBatch();
    CompletableFuture<WindmillStreamShutdownException> waitFuture =
        CompletableFuture.supplyAsync(
            () ->
                assertThrows(
                    WindmillStreamShutdownException.class,
                    queuedBatch::waitForSendOrFailNotification));
    // Wait a few seconds for the above future to get scheduled and run.
    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
    queuedBatch.notifyFailed();
    waitFuture.join();
  }
}
