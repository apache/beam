/*
 * Copyright (C) 2015 Google Inc.
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
 */
package com.google.cloud.dataflow.sdk.util.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.runners.worker.MetricTrackingWindmillServerStub;
import com.google.cloud.dataflow.sdk.runners.worker.StreamingDataflowWorker;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.KeyedGetDataRequest;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.Output;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link WindmillStateReader}.
 */
@RunWith(JUnit4.class)
public class WindmillStateReaderTest {

  private static final VarIntCoder INT_CODER = VarIntCoder.of();

  private static final String COMPUTATION = "computation";
  private static final ByteString DATA_KEY = ByteString.copyFromUtf8("DATA_KEY");
  private static final long WORK_TOKEN = 5043L;

  private static final ByteString STATE_KEY_1 = ByteString.copyFromUtf8("key1");
  private static final ByteString STATE_KEY_2 = ByteString.copyFromUtf8("key2");
  private static final String STATE_FAMILY = "family";

  @Mock
  private MetricTrackingWindmillServerStub mockWindmill;

  private WindmillStateReader underTest;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    underTest = new WindmillStateReader(mockWindmill, COMPUTATION, DATA_KEY, WORK_TOKEN);
  }

  private Windmill.Value intValue(int value, boolean padded) throws IOException {
    Output output = ByteString.newOutput();

    if (padded) {
      byte[] zero = {0x0};
      output.write(zero);
    }
    INT_CODER.encode(value, output, Coder.Context.OUTER);

    return Windmill.Value.newBuilder()
        .setData(output.toByteString())
        .setTimestamp(TimeUnit.MILLISECONDS.toMicros(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()))
        .build();
  }

  @Test
  public void testReadList() throws Exception {
    Future<Iterable<Integer>> future = underTest.listFuture(STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.GetDataRequest.Builder expectedRequest = Windmill.GetDataRequest.newBuilder();
    expectedRequest
        .addRequestsBuilder().setComputationId(COMPUTATION)
        .addRequestsBuilder().setKey(DATA_KEY).setWorkToken(WORK_TOKEN)
        .addListsToFetch(Windmill.TagList.newBuilder()
            .setTag(STATE_KEY_1).setStateFamily(STATE_FAMILY).setEndTimestamp(Long.MAX_VALUE));

    Windmill.GetDataResponse.Builder response = Windmill.GetDataResponse.newBuilder();
    response
        .addDataBuilder().setComputationId(COMPUTATION)
        .addDataBuilder().setKey(DATA_KEY)
        .addLists(Windmill.TagList.newBuilder()
            .setTag(STATE_KEY_1)
            .setStateFamily(STATE_FAMILY)
            .addValues(intValue(5, true))
            .addValues(intValue(6, true)));

    Mockito.when(mockWindmill.getStateData(expectedRequest.build())).thenReturn(response.build());

    Iterable<Integer> results = future.get();
    Mockito.verify(mockWindmill).getStateData(expectedRequest.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(results, Matchers.containsInAnyOrder(5, 6));
  }

  @Test
  public void testReadValue() throws Exception {
    Future<Integer> future = underTest.valueFuture(STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.GetDataRequest.Builder expectedRequest = Windmill.GetDataRequest.newBuilder();
    expectedRequest
        .addRequestsBuilder()
        .setComputationId(COMPUTATION)
        .addRequestsBuilder()
        .setKey(DATA_KEY)
        .setWorkToken(WORK_TOKEN)
        .addValuesToFetch(
            Windmill.TagValue.newBuilder()
                .setTag(STATE_KEY_1)
                .setStateFamily(STATE_FAMILY)
                .build());
    Windmill.GetDataResponse.Builder response = Windmill.GetDataResponse.newBuilder();
    response
        .addDataBuilder()
        .setComputationId(COMPUTATION)
        .addDataBuilder()
        .setKey(DATA_KEY)
        .addValues(
            Windmill.TagValue.newBuilder()
                .setTag(STATE_KEY_1)
                .setStateFamily(STATE_FAMILY)
                .setValue(intValue(8, false)));

    Mockito.when(mockWindmill.getStateData(expectedRequest.build())).thenReturn(response.build());

    Integer result = future.get();
    Mockito.verify(mockWindmill).getStateData(expectedRequest.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(result, Matchers.equalTo(8));
  }

  @Test
  public void testReadWatermark() throws Exception {
    Future<Instant> future = underTest.watermarkFuture(STATE_KEY_1, STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.GetDataRequest.Builder expectedRequest = Windmill.GetDataRequest.newBuilder();
    expectedRequest.addRequestsBuilder()
        .setComputationId(COMPUTATION)
        .addRequestsBuilder()
        .setKey(DATA_KEY)
        .setWorkToken(WORK_TOKEN)
        .addWatermarkHoldsToFetch(
            Windmill.WatermarkHold.newBuilder().setTag(STATE_KEY_1).setStateFamily(STATE_FAMILY));

    Windmill.GetDataResponse.Builder response = Windmill.GetDataResponse.newBuilder();
    response
        .addDataBuilder().setComputationId(COMPUTATION)
        .addDataBuilder().setKey(DATA_KEY)
        .addWatermarkHolds(Windmill.WatermarkHold.newBuilder()
            .setTag(STATE_KEY_1)
            .setStateFamily(STATE_FAMILY)
            .addTimestamps(5000000)
            .addTimestamps(6000000));

    Mockito.when(mockWindmill.getStateData(expectedRequest.build())).thenReturn(response.build());

    Instant result = future.get();
    Mockito.verify(mockWindmill).getStateData(expectedRequest.build());

    assertThat(result, Matchers.equalTo(new Instant(5000)));
  }

  @Test
  public void testBatching() throws Exception {
    // Reads two lists and verifies that we batch them up correctly.
    Future<Instant> watermarkFuture = underTest.watermarkFuture(STATE_KEY_2, STATE_FAMILY);
    Future<Iterable<Integer>> listFuture =
        underTest.listFuture(STATE_KEY_1, STATE_FAMILY, INT_CODER);

    Mockito.verifyNoMoreInteractions(mockWindmill);

    ArgumentCaptor<Windmill.GetDataRequest> request =
        ArgumentCaptor.forClass(Windmill.GetDataRequest.class);

    Windmill.GetDataResponse.Builder response = Windmill.GetDataResponse.newBuilder();
    response
        .addDataBuilder().setComputationId(COMPUTATION)
        .addDataBuilder().setKey(DATA_KEY)
        .addWatermarkHolds(Windmill.WatermarkHold.newBuilder()
            .setTag(STATE_KEY_2)
            .setStateFamily(STATE_FAMILY)
            .addTimestamps(5000000)
            .addTimestamps(6000000))
        .addLists(Windmill.TagList.newBuilder()
            .setTag(STATE_KEY_1)
            .setStateFamily(STATE_FAMILY)
            .addValues(intValue(5, true))
            .addValues(intValue(100, true)));

    Mockito.when(mockWindmill.getStateData(Mockito.isA(Windmill.GetDataRequest.class)))
        .thenReturn(response.build());
    Instant result = watermarkFuture.get();
    Mockito.verify(mockWindmill).getStateData(request.capture());

    // Verify the request looks right.
    assertThat(request.getValue().getRequestsCount(), Matchers.equalTo(1));
    assertThat(request.getValue().getRequests(0).getComputationId(), Matchers.equalTo(COMPUTATION));
    assertThat(request.getValue().getRequests(0).getRequestsCount(), Matchers.equalTo(1));
    KeyedGetDataRequest keyedRequest = request.getValue().getRequests(0).getRequests(0);
    assertThat(keyedRequest.getKey(), Matchers.equalTo(DATA_KEY));
    assertThat(keyedRequest.getWorkToken(), Matchers.equalTo(WORK_TOKEN));
    assertThat(keyedRequest.getListsToFetchCount(), Matchers.equalTo(1));
    assertThat(keyedRequest.getListsToFetch(0).getEndTimestamp(), Matchers.equalTo(Long.MAX_VALUE));
    assertThat(keyedRequest.getListsToFetch(0).getTag(), Matchers.equalTo(STATE_KEY_1));
    assertThat(keyedRequest.getWatermarkHoldsToFetchCount(), Matchers.equalTo(1));
    assertThat(keyedRequest.getWatermarkHoldsToFetch(0).getTag(), Matchers.equalTo(STATE_KEY_2));

    // Verify the values returned to the user.
    assertThat(result, Matchers.equalTo(new Instant(5000)));
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(listFuture.get(), Matchers.containsInAnyOrder(5, 100));
    Mockito.verifyNoMoreInteractions(mockWindmill);

    // And verify that getting a future again returns the already completed future.
    Future<Instant> watermarkFuture2 = underTest.watermarkFuture(STATE_KEY_2, STATE_FAMILY);
    assertTrue(watermarkFuture2.isDone());
  }

  @Test
  public void testNoStateFamily() throws Exception {
    Future<Integer> future = underTest.valueFuture(STATE_KEY_1, "", INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.GetDataRequest.Builder expectedRequest = Windmill.GetDataRequest.newBuilder();
    expectedRequest
        .addRequestsBuilder()
        .setComputationId(COMPUTATION)
        .addRequestsBuilder()
        .setKey(DATA_KEY)
        .setWorkToken(WORK_TOKEN)
        .addValuesToFetch(
            Windmill.TagValue.newBuilder()
                .setTag(STATE_KEY_1)
                .setStateFamily("")
                .build());
    Windmill.GetDataResponse.Builder response = Windmill.GetDataResponse.newBuilder();
    response
        .addDataBuilder()
        .setComputationId(COMPUTATION)
        .addDataBuilder()
        .setKey(DATA_KEY)
        .addValues(
            Windmill.TagValue.newBuilder()
                .setTag(STATE_KEY_1)
                .setStateFamily("")
                .setValue(intValue(8, false)));

    Mockito.when(mockWindmill.getStateData(expectedRequest.build())).thenReturn(response.build());

    Integer result = future.get();
    Mockito.verify(mockWindmill).getStateData(expectedRequest.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(result, Matchers.equalTo(8));

  }

  @Test
  public void testKeyTokenInvalid() throws Exception {
    // Reads two lists and verifies that we batch them up correctly.
    Future<Instant> watermarkFuture = underTest.watermarkFuture(STATE_KEY_2, STATE_FAMILY);
    Future<Iterable<Integer>> listFuture =
        underTest.listFuture(STATE_KEY_1, STATE_FAMILY, INT_CODER);

    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.GetDataResponse.Builder response = Windmill.GetDataResponse.newBuilder();
    response
        .addDataBuilder().setComputationId(COMPUTATION)
        .addDataBuilder().setKey(DATA_KEY).setFailed(true);

    Mockito.when(mockWindmill.getStateData(Mockito.isA(Windmill.GetDataRequest.class)))
        .thenReturn(response.build());

    try {
      watermarkFuture.get();
      fail("Expected KeyTokenInvalidException");
    } catch (Throwable e) {
      assertTrue(StreamingDataflowWorker.isKeyTokenInvalidException(e));
    }

    try {
      listFuture.get();
      fail("Expected KeyTokenInvalidException");
    } catch (Throwable e) {
      assertTrue(StreamingDataflowWorker.isKeyTokenInvalidException(e));
    }
  }

  /**
   * Tests that multiple reads for the same tag in the same batch are cached. We can't compare
   * the futures since we've wrapped the delegate aronud them, so we just verify there is only
   * one queued lookup.
   */
  @Test
  public void testCachingWithinBatch() throws Exception {
    underTest.watermarkFuture(STATE_KEY_1, STATE_FAMILY);
    underTest.watermarkFuture(STATE_KEY_1, STATE_FAMILY);
    assertEquals(1, underTest.pendingLookups.size());
  }
}
