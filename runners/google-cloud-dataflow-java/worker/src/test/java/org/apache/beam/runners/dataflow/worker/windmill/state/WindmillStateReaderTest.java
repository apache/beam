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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import org.apache.beam.runners.dataflow.worker.KeyTokenInvalidException;
import org.apache.beam.runners.dataflow.worker.WindmillStateTestUtils;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.SortedListEntry;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.SortedListRange;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link WindmillStateReader}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "FutureReturnValueIgnored",
})
public class WindmillStateReaderTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private static final VarIntCoder INT_CODER = VarIntCoder.of();

  private static final String COMPUTATION = "computation";
  private static final ByteString DATA_KEY = ByteString.copyFromUtf8("DATA_KEY");
  private static final long SHARDING_KEY = 17L;
  private static final long WORK_TOKEN = 5043L;
  private static final long CONT_POSITION = 1391631351L;

  private static final ByteString STATE_KEY_PREFIX = ByteString.copyFromUtf8("key");
  private static final ByteString STATE_MULTIMAP_KEY_1 = ByteString.copyFromUtf8("multimapkey1");
  private static final ByteString STATE_MULTIMAP_KEY_2 = ByteString.copyFromUtf8("multimapkey2");
  private static final ByteString STATE_MULTIMAP_KEY_3 = ByteString.copyFromUtf8("multimapkey3");
  private static final ByteString STATE_MULTIMAP_CONT_1 = ByteString.copyFromUtf8("continuation_1");
  private static final ByteString STATE_MULTIMAP_CONT_2 = ByteString.copyFromUtf8("continuation_2");
  private static final ByteString STATE_KEY_1 = ByteString.copyFromUtf8("key1");
  private static final ByteString STATE_KEY_2 = ByteString.copyFromUtf8("key2");
  private static final String STATE_FAMILY = "family";

  private static final String STATE_FAMILY2 = "family2";

  private static void assertNoReader(Object obj) throws Exception {
    WindmillStateTestUtils.assertNoReference(obj, WindmillStateReader.class);
  }

  @Mock private GetDataClient mockWindmill;

  private WindmillStateReader underTest;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    underTest =
        WindmillStateReader.forTesting(
            (request) -> Optional.ofNullable(mockWindmill.getStateData(COMPUTATION, request)),
            DATA_KEY,
            SHARDING_KEY,
            WORK_TOKEN);
  }

  private Windmill.Value intValue(int value) throws IOException {
    return Windmill.Value.newBuilder()
        .setData(intData(value))
        .setTimestamp(
            WindmillTimeUtils.harnessToWindmillTimestamp(BoundedWindow.TIMESTAMP_MAX_VALUE))
        .build();
  }

  private ByteString intData(int value) throws IOException {
    ByteStringOutputStream output = new ByteStringOutputStream();
    INT_CODER.encode(value, output, Coder.Context.OUTER);
    return output.toByteString();
  }

  @Test
  public void testReadMultimapSingleEntry() throws Exception {
    Future<Iterable<Integer>> future =
        underTest.multimapFetchSingleEntryFuture(
            STATE_MULTIMAP_KEY_1, STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(false)
                    .addEntriesToFetch(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_MULTIMAP_BYTES)
                            .build()));

    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .addAllValues(Arrays.asList(intData(5), intData(6)))));
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest.build()))
        .thenReturn(response.build());

    Iterable<Integer> results = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest.build());

    assertThat(results, Matchers.containsInAnyOrder(5, 6));
    Mockito.verifyNoMoreInteractions(mockWindmill);
    assertNoReader(future);
  }

  @Test
  public void testReadMultimapSingleEntryPaginated() throws Exception {
    Future<Iterable<Integer>> future =
        underTest.multimapFetchSingleEntryFuture(
            STATE_MULTIMAP_KEY_1, STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest1 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(false)
                    .addEntriesToFetch(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_MULTIMAP_BYTES)
                            .build()));

    Windmill.KeyedGetDataResponse.Builder response1 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .addAllValues(Arrays.asList(intData(5), intData(6)))
                            .setContinuationPosition(500)));
    Windmill.KeyedGetDataRequest.Builder expectedRequest2 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_CONTINUATION_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(false)
                    .addEntriesToFetch(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .setFetchMaxBytes(WindmillStateReader.CONTINUATION_MAX_MULTIMAP_BYTES)
                            .setRequestPosition(500)
                            .build()));

    Windmill.KeyedGetDataResponse.Builder response2 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .addAllValues(Arrays.asList(intData(7), intData(8)))
                            .setContinuationPosition(800)
                            .setRequestPosition(500)));
    Windmill.KeyedGetDataRequest.Builder expectedRequest3 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_CONTINUATION_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(false)
                    .addEntriesToFetch(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .setFetchMaxBytes(WindmillStateReader.CONTINUATION_MAX_MULTIMAP_BYTES)
                            .setRequestPosition(800)
                            .build()));

    Windmill.KeyedGetDataResponse.Builder response3 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .addAllValues(Arrays.asList(intData(9), intData(10)))
                            .setRequestPosition(800)));
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest1.build()))
        .thenReturn(response1.build());
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest2.build()))
        .thenReturn(response2.build());
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest3.build()))
        .thenReturn(response3.build());

    Iterable<Integer> results = future.get();

    assertThat(results, Matchers.contains(5, 6, 7, 8, 9, 10));

    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest1.build());
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest2.build());
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest3.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);
    // NOTE: The future will still contain a reference to the underlying reader, thus not calling
    // assertNoReader(future).
  }

  // check whether the two TagMultimapFetchRequests equal to each other, ignoring the order of
  // entries and the order of values in each entry.
  private static void assertMultimapFetchRequestEqual(
      Windmill.TagMultimapFetchRequest req1, Windmill.TagMultimapFetchRequest req2) {
    assertMultimapEntriesEqual(req1.getEntriesToFetchList(), req2.getEntriesToFetchList());
    assertEquals(
        req1.toBuilder().clearEntriesToFetch().build(),
        req2.toBuilder().clearEntriesToFetch().build());
  }

  private static void assertMultimapEntriesEqual(
      List<Windmill.TagMultimapEntry> left, List<Windmill.TagMultimapEntry> right) {
    Map<ByteString, Windmill.TagMultimapEntry> map = Maps.newHashMap();
    for (Windmill.TagMultimapEntry entry : left) {
      map.put(entry.getEntryName(), entry);
    }
    for (Windmill.TagMultimapEntry entry : right) {
      assertTrue(map.containsKey(entry.getEntryName()));
      Windmill.TagMultimapEntry that = map.remove(entry.getEntryName());
      if (entry.getValuesCount() == 0) {
        assertEquals(0, that.getValuesCount());
      } else {
        assertThat(entry.getValuesList(), Matchers.containsInAnyOrder(that.getValuesList()));
      }
      assertEquals(entry.toBuilder().clearValues().build(), that.toBuilder().clearValues().build());
    }
    assertTrue(map.isEmpty());
  }

  @Test
  public void testReadMultimapMultipleEntries() throws Exception {
    Future<Iterable<Integer>> future1 =
        underTest.multimapFetchSingleEntryFuture(
            STATE_MULTIMAP_KEY_1, STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Future<Iterable<Integer>> future2 =
        underTest.multimapFetchSingleEntryFuture(
            STATE_MULTIMAP_KEY_2, STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(false)
                    .addEntriesToFetch(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_MULTIMAP_BYTES)
                            .build())
                    .addEntriesToFetch(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_2)
                            .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_MULTIMAP_BYTES)
                            .build()));

    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .addAllValues(Arrays.asList(intData(5), intData(6))))
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_2)
                            .addAllValues(Arrays.asList(intData(15), intData(16)))));
    when(mockWindmill.getStateData(ArgumentMatchers.eq(COMPUTATION), ArgumentMatchers.any()))
        .thenReturn(response.build());

    Iterable<Integer> results1 = future1.get();
    Iterable<Integer> results2 = future2.get();

    final ArgumentCaptor<Windmill.KeyedGetDataRequest> requestCaptor =
        ArgumentCaptor.forClass(Windmill.KeyedGetDataRequest.class);
    Mockito.verify(mockWindmill)
        .getStateData(ArgumentMatchers.eq(COMPUTATION), requestCaptor.capture());
    assertMultimapFetchRequestEqual(
        expectedRequest.build().getMultimapsToFetch(0),
        requestCaptor.getValue().getMultimapsToFetch(0));

    assertThat(results1, Matchers.containsInAnyOrder(5, 6));
    assertThat(results2, Matchers.containsInAnyOrder(15, 16));

    Mockito.verifyNoMoreInteractions(mockWindmill);
    assertNoReader(future1);
    assertNoReader(future2);
  }

  @Test
  public void testReadMultimapMultipleEntriesWithPagination() throws Exception {
    Future<Iterable<Integer>> future1 =
        underTest.multimapFetchSingleEntryFuture(
            STATE_MULTIMAP_KEY_1, STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Future<Iterable<Integer>> future2 =
        underTest.multimapFetchSingleEntryFuture(
            STATE_MULTIMAP_KEY_2, STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest1 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(false)
                    .addEntriesToFetch(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_MULTIMAP_BYTES)
                            .build())
                    .addEntriesToFetch(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_2)
                            .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_MULTIMAP_BYTES)
                            .build()));

    Windmill.KeyedGetDataResponse.Builder response1 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .addAllValues(Arrays.asList(intData(5), intData(6)))
                            .setContinuationPosition(800))
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_2)
                            .addAllValues(Arrays.asList(intData(15), intData(16)))));
    Windmill.KeyedGetDataRequest.Builder expectedRequest2 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_CONTINUATION_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(false)
                    .addEntriesToFetch(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .setFetchMaxBytes(WindmillStateReader.CONTINUATION_MAX_MULTIMAP_BYTES)
                            .setRequestPosition(800)
                            .build()));
    Windmill.KeyedGetDataResponse.Builder response2 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .addAllValues(Arrays.asList(intData(7), intData(8)))
                            .setRequestPosition(800)));
    when(mockWindmill.getStateData(ArgumentMatchers.eq(COMPUTATION), ArgumentMatchers.any()))
        .thenReturn(response1.build())
        .thenReturn(response2.build());

    Iterable<Integer> results1 = future1.get();
    Iterable<Integer> results2 = future2.get();

    assertThat(results1, Matchers.containsInAnyOrder(5, 6, 7, 8));
    assertThat(results2, Matchers.containsInAnyOrder(15, 16));

    final ArgumentCaptor<Windmill.KeyedGetDataRequest> requestCaptor =
        ArgumentCaptor.forClass(Windmill.KeyedGetDataRequest.class);
    Mockito.verify(mockWindmill, times(2))
        .getStateData(ArgumentMatchers.eq(COMPUTATION), requestCaptor.capture());
    assertMultimapFetchRequestEqual(
        expectedRequest1.build().getMultimapsToFetch(0),
        requestCaptor.getAllValues().get(0).getMultimapsToFetch(0));
    assertMultimapFetchRequestEqual(
        expectedRequest2.build().getMultimapsToFetch(0),
        requestCaptor.getAllValues().get(1).getMultimapsToFetch(0));
    Mockito.verifyNoMoreInteractions(mockWindmill);
    // NOTE: The future will still contain a reference to the underlying reader, thus not calling
    // assertNoReader(future).
  }

  @Test
  public void testReadMultimapKeys() throws Exception {
    Future<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> future =
        underTest.multimapFetchAllFuture(true, STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(true)
                    .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_MULTIMAP_BYTES));

    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder().setEntryName(STATE_MULTIMAP_KEY_1))
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder().setEntryName(STATE_MULTIMAP_KEY_2)));
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest.build()))
        .thenReturn(response.build());

    Iterable<Map.Entry<ByteString, Iterable<Integer>>> results = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest.build());
    List<ByteString> keys = Lists.newArrayList();
    for (Map.Entry<ByteString, Iterable<Integer>> entry : results) {
      keys.add(entry.getKey());
      assertEquals(0, Iterables.size(entry.getValue()));
    }
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(keys, Matchers.containsInAnyOrder(STATE_MULTIMAP_KEY_1, STATE_MULTIMAP_KEY_2));
    assertNoReader(future);
  }

  @Test
  public void testReadMultimapKeysPaginated() throws Exception {
    Future<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> future =
        underTest.multimapFetchAllFuture(true, STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest1 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(true)
                    .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_MULTIMAP_BYTES));

    Windmill.KeyedGetDataResponse.Builder response1 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder().setEntryName(STATE_MULTIMAP_KEY_1))
                    .setContinuationPosition(STATE_MULTIMAP_CONT_1));

    Windmill.KeyedGetDataRequest.Builder expectedRequest2 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_CONTINUATION_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(true)
                    .setFetchMaxBytes(WindmillStateReader.CONTINUATION_MAX_MULTIMAP_BYTES)
                    .setRequestPosition(STATE_MULTIMAP_CONT_1));

    Windmill.KeyedGetDataResponse.Builder response2 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder().setEntryName(STATE_MULTIMAP_KEY_2))
                    .setRequestPosition(STATE_MULTIMAP_CONT_1)
                    .setContinuationPosition(STATE_MULTIMAP_CONT_2));
    Windmill.KeyedGetDataRequest.Builder expectedRequest3 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_CONTINUATION_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(true)
                    .setFetchMaxBytes(WindmillStateReader.CONTINUATION_MAX_MULTIMAP_BYTES)
                    .setRequestPosition(STATE_MULTIMAP_CONT_2));

    Windmill.KeyedGetDataResponse.Builder response3 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder().setEntryName(STATE_MULTIMAP_KEY_3))
                    .setRequestPosition(STATE_MULTIMAP_CONT_2));
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest1.build()))
        .thenReturn(response1.build());
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest2.build()))
        .thenReturn(response2.build());
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest3.build()))
        .thenReturn(response3.build());

    Iterable<Map.Entry<ByteString, Iterable<Integer>>> results = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest1.build());
    List<ByteString> keys = Lists.newArrayList();
    for (Map.Entry<ByteString, Iterable<Integer>> entry : results) {
      keys.add(entry.getKey());
      assertEquals(0, Iterables.size(entry.getValue()));
    }
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest2.build());
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest3.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(
        keys,
        Matchers.containsInAnyOrder(
            STATE_MULTIMAP_KEY_1, STATE_MULTIMAP_KEY_2, STATE_MULTIMAP_KEY_3));
    // NOTE: The future will still contain a reference to the underlying reader, thus not calling
    // assertNoReader(future).
  }

  @Test
  public void testReadMultimapAllEntries() throws Exception {
    Future<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> future =
        underTest.multimapFetchAllFuture(false, STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(false)
                    .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_MULTIMAP_BYTES));

    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .addValues(intData(1))
                            .addValues(intData(2)))
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_2)
                            .addValues(intData(10))
                            .addValues(intData(20))));
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest.build()))
        .thenReturn(response.build());

    Iterable<Map.Entry<ByteString, Iterable<Integer>>> results = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest.build());
    int foundEntries = 0;
    for (Map.Entry<ByteString, Iterable<Integer>> entry : results) {
      if (entry.getKey().equals(STATE_MULTIMAP_KEY_1)) {
        foundEntries++;
        assertThat(entry.getValue(), Matchers.containsInAnyOrder(1, 2));
      } else {
        foundEntries++;
        assertEquals(STATE_MULTIMAP_KEY_2, entry.getKey());
        assertThat(entry.getValue(), Matchers.containsInAnyOrder(10, 20));
      }
    }
    assertEquals(2, foundEntries);
    Mockito.verifyNoMoreInteractions(mockWindmill);
    assertNoReader(future);
  }

  private static void assertMultimapEntries(
      Iterable<Map.Entry<ByteString, Iterable<Integer>>> expected,
      List<Map.Entry<ByteString, List<Integer>>> actual) {
    Map<ByteString, List<Integer>> expectedMap = Maps.newHashMap();
    for (Map.Entry<ByteString, Iterable<Integer>> entry : expected) {
      ByteString key = entry.getKey();
      if (!expectedMap.containsKey(key)) expectedMap.put(key, new ArrayList<>());
      entry.getValue().forEach(expectedMap.get(key)::add);
    }
    for (Map.Entry<ByteString, List<Integer>> entry : actual) {
      assertTrue(expectedMap.containsKey(entry.getKey()));
      assertThat(
          entry.getValue(),
          Matchers.containsInAnyOrder(expectedMap.remove(entry.getKey()).toArray()));
    }
    assertTrue(expectedMap.isEmpty());
  }

  @Test
  public void testReadMultimapEntriesPaginated() throws Exception {
    Future<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> future =
        underTest.multimapFetchAllFuture(false, STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest1 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(false)
                    .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_MULTIMAP_BYTES));

    Windmill.KeyedGetDataResponse.Builder response1 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_1)
                            .addValues(intData(1))
                            .addValues(intData(2)))
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_2)
                            .addValues(intData(3))
                            .addValues(intData(3)))
                    .setContinuationPosition(STATE_MULTIMAP_CONT_1));

    Windmill.KeyedGetDataRequest.Builder expectedRequest2 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_CONTINUATION_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(false)
                    .setFetchMaxBytes(WindmillStateReader.CONTINUATION_MAX_MULTIMAP_BYTES)
                    .setRequestPosition(STATE_MULTIMAP_CONT_1));

    Windmill.KeyedGetDataResponse.Builder response2 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_2)
                            .addValues(intData(2)))
                    .setRequestPosition(STATE_MULTIMAP_CONT_1)
                    .setContinuationPosition(STATE_MULTIMAP_CONT_2));
    Windmill.KeyedGetDataRequest.Builder expectedRequest3 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_CONTINUATION_KEY_BYTES)
            .addMultimapsToFetch(
                Windmill.TagMultimapFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchEntryNamesOnly(false)
                    .setFetchMaxBytes(WindmillStateReader.CONTINUATION_MAX_MULTIMAP_BYTES)
                    .setRequestPosition(STATE_MULTIMAP_CONT_2));

    Windmill.KeyedGetDataResponse.Builder response3 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagMultimaps(
                Windmill.TagMultimapFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        Windmill.TagMultimapEntry.newBuilder()
                            .setEntryName(STATE_MULTIMAP_KEY_2)
                            .addValues(intData(4)))
                    .setRequestPosition(STATE_MULTIMAP_CONT_2));
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest1.build()))
        .thenReturn(response1.build());
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest2.build()))
        .thenReturn(response2.build());
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest3.build()))
        .thenReturn(response3.build());

    Iterable<Map.Entry<ByteString, Iterable<Integer>>> results = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest1.build());
    assertMultimapEntries(
        results,
        Arrays.asList(
            new AbstractMap.SimpleEntry<>(STATE_MULTIMAP_KEY_1, Arrays.asList(1, 2)),
            new AbstractMap.SimpleEntry<>(STATE_MULTIMAP_KEY_2, Arrays.asList(3, 3, 2, 4))));
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest2.build());
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest3.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);
    // NOTE: The future will still contain a reference to the underlying reader , thus not calling
    // assertNoReader(future).
  }

  @Test
  public void testReadBag() throws Exception {
    Future<Iterable<Integer>> future = underTest.bagFuture(STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addBagsToFetch(
                Windmill.TagBag.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_BAG_BYTES));

    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addBags(
                Windmill.TagBag.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addValues(intData(5))
                    .addValues(intData(6)));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest.build()))
        .thenReturn(response.build());

    Iterable<Integer> results = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest.build());
    for (Integer unused : results) {
      // Iterate over the results to force loading all the pages.
    }
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(results, Matchers.contains(5, 6));
    assertNoReader(future);
  }

  @Test
  public void testReadBagWithContinuations() throws Exception {
    Future<Iterable<Integer>> future = underTest.bagFuture(STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest1 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addBagsToFetch(
                Windmill.TagBag.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_BAG_BYTES));

    Windmill.KeyedGetDataResponse.Builder response1 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addBags(
                Windmill.TagBag.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setContinuationPosition(CONT_POSITION)
                    .addValues(intData(5))
                    .addValues(intData(6)));

    Windmill.KeyedGetDataRequest.Builder expectedRequest2 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_CONTINUATION_KEY_BYTES)
            .addBagsToFetch(
                Windmill.TagBag.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchMaxBytes(WindmillStateReader.CONTINUATION_MAX_BAG_BYTES)
                    .setRequestPosition(CONT_POSITION));

    Windmill.KeyedGetDataResponse.Builder response2 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addBags(
                Windmill.TagBag.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setRequestPosition(CONT_POSITION)
                    .addValues(intData(7))
                    .addValues(intData(8)));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest1.build()))
        .thenReturn(response1.build());
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest2.build()))
        .thenReturn(response2.build());

    Iterable<Integer> results = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest1.build());
    for (Integer unused : results) {
      // Iterate over the results to force loading all the pages.
    }
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest2.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(results, Matchers.contains(5, 6, 7, 8));
    // NOTE: The future will still contain a reference to the underlying reader , thus not calling
    // assertNoReader(future).
  }

  @Test
  public void testReadSortedList() throws Exception {
    long beginning = SortedListRange.getDefaultInstance().getStart();
    long end = SortedListRange.getDefaultInstance().getLimit();
    Future<Iterable<TimestampedValue<Integer>>> future =
        underTest.orderedListFuture(
            Range.closedOpen(beginning, end), STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    // Fetch the entire list.
    Windmill.KeyedGetDataRequest.Builder expectedRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addSortedListsToFetch(
                Windmill.TagSortedListFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(beginning).setLimit(end))
                    .setFetchMaxBytes(WindmillStateReader.MAX_ORDERED_LIST_BYTES));

    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagSortedLists(
                Windmill.TagSortedListFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(5)).setSortKey(5000).setId(5))
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(6)).setSortKey(6000).setId(5))
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(7)).setSortKey(7000).setId(7))
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(8)).setSortKey(8000).setId(8))
                    .addFetchRanges(
                        SortedListRange.newBuilder().setStart(beginning).setLimit(end)));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest.build()))
        .thenReturn(response.build());

    Iterable<TimestampedValue<Integer>> results = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest.build());
    for (TimestampedValue<Integer> unused : results) {
      // Iterate over the results to force loading all the pages.
    }
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(
        results,
        Matchers.contains(
            TimestampedValue.of(5, Instant.ofEpochMilli(5)),
            TimestampedValue.of(6, Instant.ofEpochMilli(6)),
            TimestampedValue.of(7, Instant.ofEpochMilli(7)),
            TimestampedValue.of(8, Instant.ofEpochMilli(8))));
    assertNoReader(future);
  }

  @Test
  public void testReadSortedListRanges() throws Exception {
    Future<Iterable<TimestampedValue<Integer>>> future1 =
        underTest.orderedListFuture(Range.closedOpen(0L, 5L), STATE_KEY_1, STATE_FAMILY, INT_CODER);
    // Should be put into a subsequent batch as it has the same key and state family.
    Future<Iterable<TimestampedValue<Integer>>> future2 =
        underTest.orderedListFuture(Range.closedOpen(5L, 6L), STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Future<Iterable<TimestampedValue<Integer>>> future3 =
        underTest.orderedListFuture(
            Range.closedOpen(6L, 10L), STATE_KEY_2, STATE_FAMILY, INT_CODER);
    Future<Iterable<TimestampedValue<Integer>>> future4 =
        underTest.orderedListFuture(
            Range.closedOpen(11L, 12L), STATE_KEY_2, STATE_FAMILY2, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    // Fetch the entire list.
    Windmill.KeyedGetDataRequest.Builder expectedRequest1 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addSortedListsToFetch(
                Windmill.TagSortedListFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(0).setLimit(5))
                    .setFetchMaxBytes(WindmillStateReader.MAX_ORDERED_LIST_BYTES))
            .addSortedListsToFetch(
                Windmill.TagSortedListFetchRequest.newBuilder()
                    .setTag(STATE_KEY_2)
                    .setStateFamily(STATE_FAMILY)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(6).setLimit(10))
                    .setFetchMaxBytes(WindmillStateReader.MAX_ORDERED_LIST_BYTES))
            .addSortedListsToFetch(
                Windmill.TagSortedListFetchRequest.newBuilder()
                    .setTag(STATE_KEY_2)
                    .setStateFamily(STATE_FAMILY2)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(11).setLimit(12))
                    .setFetchMaxBytes(WindmillStateReader.MAX_ORDERED_LIST_BYTES));

    Windmill.KeyedGetDataRequest.Builder expectedRequest2 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addSortedListsToFetch(
                Windmill.TagSortedListFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(5).setLimit(6))
                    .setFetchMaxBytes(WindmillStateReader.MAX_ORDERED_LIST_BYTES));

    Windmill.KeyedGetDataResponse.Builder response1 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagSortedLists(
                Windmill.TagSortedListFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(5)).setSortKey(5000).setId(5))
                    .addFetchRanges(SortedListRange.newBuilder().setStart(0).setLimit(5)))
            .addTagSortedLists(
                Windmill.TagSortedListFetchResponse.newBuilder()
                    .setTag(STATE_KEY_2)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(8)).setSortKey(8000).setId(8))
                    .addFetchRanges(SortedListRange.newBuilder().setStart(6).setLimit(10)))
            .addTagSortedLists(
                Windmill.TagSortedListFetchResponse.newBuilder()
                    .setTag(STATE_KEY_2)
                    .setStateFamily(STATE_FAMILY2)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(11).setLimit(12)));

    Windmill.KeyedGetDataResponse.Builder response2 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagSortedLists(
                Windmill.TagSortedListFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(6)).setSortKey(6000).setId(5))
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(7)).setSortKey(7000).setId(7))
                    .addFetchRanges(SortedListRange.newBuilder().setStart(5).setLimit(6)));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest1.build()))
        .thenReturn(response1.build());
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest2.build()))
        .thenReturn(response2.build());

    // Trigger reads of batching. By fetching future2 which is not part of the first batch we ensure
    // that all batches are fetched.
    {
      Iterable<TimestampedValue<Integer>> results = future2.get();
      Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest1.build());
      Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest2.build());
      Mockito.verifyNoMoreInteractions(mockWindmill);
      assertThat(
          results,
          Matchers.contains(
              TimestampedValue.of(6, Instant.ofEpochMilli(6)),
              TimestampedValue.of(7, Instant.ofEpochMilli(7))));
      assertNoReader(future2);
    }

    {
      Iterable<TimestampedValue<Integer>> results = future1.get();
      assertThat(results, Matchers.contains(TimestampedValue.of(5, Instant.ofEpochMilli(5))));
      assertNoReader(future1);
    }

    {
      Iterable<TimestampedValue<Integer>> results = future3.get();
      assertThat(results, Matchers.contains(TimestampedValue.of(8, Instant.ofEpochMilli(8))));
      assertNoReader(future3);
    }

    {
      Iterable<TimestampedValue<Integer>> results = future4.get();
      assertThat(results, Matchers.emptyIterable());
      assertNoReader(future4);
    }
  }

  @Test
  public void testReadSortedListWithContinuations() throws Exception {
    long beginning = SortedListRange.getDefaultInstance().getStart();
    long end = SortedListRange.getDefaultInstance().getLimit();

    Future<Iterable<TimestampedValue<Integer>>> future =
        underTest.orderedListFuture(
            Range.closedOpen(beginning, end), STATE_KEY_1, STATE_FAMILY, INT_CODER);

    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest1 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addSortedListsToFetch(
                Windmill.TagSortedListFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(beginning).setLimit(end))
                    .setFetchMaxBytes(WindmillStateReader.MAX_ORDERED_LIST_BYTES));

    final ByteString CONT_1 = ByteString.copyFrom("CONTINUATION_1", StandardCharsets.UTF_8);
    final ByteString CONT_2 = ByteString.copyFrom("CONTINUATION_2", StandardCharsets.UTF_8);
    Windmill.KeyedGetDataResponse.Builder response1 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagSortedLists(
                Windmill.TagSortedListFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(1)).setSortKey(1000).setId(1))
                    .setContinuationPosition(CONT_1)
                    .addFetchRanges(
                        SortedListRange.newBuilder().setStart(beginning).setLimit(end)));

    Windmill.KeyedGetDataRequest.Builder expectedRequest2 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addSortedListsToFetch(
                Windmill.TagSortedListFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(beginning).setLimit(end))
                    .setRequestPosition(CONT_1)
                    .setFetchMaxBytes(WindmillStateReader.MAX_ORDERED_LIST_BYTES));

    Windmill.KeyedGetDataResponse.Builder response2 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagSortedLists(
                Windmill.TagSortedListFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(2)).setSortKey(2000).setId(2))
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(3)).setSortKey(3000).setId(3))
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(4)).setSortKey(4000).setId(4))
                    .setContinuationPosition(CONT_2)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(beginning).setLimit(end))
                    .setRequestPosition(CONT_1));

    Windmill.KeyedGetDataRequest.Builder expectedRequest3 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addSortedListsToFetch(
                Windmill.TagSortedListFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(beginning).setLimit(end))
                    .setRequestPosition(CONT_2)
                    .setFetchMaxBytes(WindmillStateReader.MAX_ORDERED_LIST_BYTES));

    Windmill.KeyedGetDataResponse.Builder response3 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagSortedLists(
                Windmill.TagSortedListFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(5)).setSortKey(5000).setId(5))
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(6)).setSortKey(6000).setId(7))
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(7)).setSortKey(7000).setId(7))
                    .addFetchRanges(SortedListRange.newBuilder().setStart(beginning).setLimit(end))
                    .setRequestPosition(CONT_2));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest1.build()))
        .thenReturn(response1.build());
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest2.build()))
        .thenReturn(response2.build());
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest3.build()))
        .thenReturn(response3.build());

    Iterable<TimestampedValue<Integer>> results = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest1.build());
    for (TimestampedValue<Integer> unused : results) {
      // Iterate over the results to force loading all the pages.
    }
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest2.build());
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest3.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(
        results,
        Matchers.contains(
            TimestampedValue.of(1, Instant.ofEpochMilli(1)),
            TimestampedValue.of(2, Instant.ofEpochMilli(2)),
            TimestampedValue.of(3, Instant.ofEpochMilli(3)),
            TimestampedValue.of(4, Instant.ofEpochMilli(4)),
            TimestampedValue.of(5, Instant.ofEpochMilli(5)),
            TimestampedValue.of(6, Instant.ofEpochMilli(6)),
            TimestampedValue.of(7, Instant.ofEpochMilli(7))));
    // NOTE: The future will still contain a reference to the underlying reader , thus not calling
    // assertNoReader(future).
  }

  @Test
  public void testReadTagValuePrefix() throws Exception {
    Future<Iterable<Map.Entry<ByteString, Integer>>> future =
        underTest.valuePrefixFuture(STATE_KEY_PREFIX, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addTagValuePrefixesToFetch(
                Windmill.TagValuePrefixRequest.newBuilder()
                    .setTagPrefix(STATE_KEY_PREFIX)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchMaxBytes(WindmillStateReader.MAX_TAG_VALUE_PREFIX_BYTES));

    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagValuePrefixes(
                Windmill.TagValuePrefixResponse.newBuilder()
                    .setTagPrefix(STATE_KEY_PREFIX)
                    .setStateFamily(STATE_FAMILY)
                    .addTagValues(
                        Windmill.TagValue.newBuilder()
                            .setTag(STATE_KEY_1)
                            .setStateFamily(STATE_FAMILY)
                            .setValue(intValue(8)))
                    .addTagValues(
                        Windmill.TagValue.newBuilder()
                            .setTag(STATE_KEY_2)
                            .setStateFamily(STATE_FAMILY)
                            .setValue(intValue(9))));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest.build()))
        .thenReturn(response.build());

    Iterable<Map.Entry<ByteString, Integer>> result = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(
        result,
        Matchers.containsInAnyOrder(
            new AbstractMap.SimpleEntry<>(STATE_KEY_1, 8),
            new AbstractMap.SimpleEntry<>(STATE_KEY_2, 9)));

    assertNoReader(future);
  }

  @Test
  public void testReadTagValuePrefixWithContinuations() throws Exception {
    Future<Iterable<Map.Entry<ByteString, Integer>>> future =
        underTest.valuePrefixFuture(STATE_KEY_PREFIX, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest1 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addTagValuePrefixesToFetch(
                Windmill.TagValuePrefixRequest.newBuilder()
                    .setTagPrefix(STATE_KEY_PREFIX)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchMaxBytes(WindmillStateReader.MAX_TAG_VALUE_PREFIX_BYTES));

    final ByteString CONT = ByteString.copyFrom("CONTINUATION", StandardCharsets.UTF_8);
    Windmill.KeyedGetDataResponse.Builder response1 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagValuePrefixes(
                Windmill.TagValuePrefixResponse.newBuilder()
                    .setTagPrefix(STATE_KEY_PREFIX)
                    .setStateFamily(STATE_FAMILY)
                    .setContinuationPosition(CONT)
                    .addTagValues(
                        Windmill.TagValue.newBuilder()
                            .setTag(STATE_KEY_1)
                            .setStateFamily(STATE_FAMILY)
                            .setValue(intValue(8))));

    Windmill.KeyedGetDataRequest.Builder expectedRequest2 =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addTagValuePrefixesToFetch(
                Windmill.TagValuePrefixRequest.newBuilder()
                    .setTagPrefix(STATE_KEY_PREFIX)
                    .setStateFamily(STATE_FAMILY)
                    .setRequestPosition(CONT)
                    .setFetchMaxBytes(WindmillStateReader.MAX_TAG_VALUE_PREFIX_BYTES));

    Windmill.KeyedGetDataResponse.Builder response2 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagValuePrefixes(
                Windmill.TagValuePrefixResponse.newBuilder()
                    .setTagPrefix(STATE_KEY_PREFIX)
                    .setStateFamily(STATE_FAMILY)
                    .setRequestPosition(CONT)
                    .addTagValues(
                        Windmill.TagValue.newBuilder()
                            .setTag(STATE_KEY_2)
                            .setStateFamily(STATE_FAMILY)
                            .setValue(intValue(9))));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest1.build()))
        .thenReturn(response1.build());
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest2.build()))
        .thenReturn(response2.build());

    Iterable<Map.Entry<ByteString, Integer>> results = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest1.build());
    for (Map.Entry<ByteString, Integer> unused : results) {
      // Iterate over the results to force loading all the pages.
    }
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest2.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(
        results,
        Matchers.containsInAnyOrder(
            new AbstractMap.SimpleEntry<>(STATE_KEY_1, 8),
            new AbstractMap.SimpleEntry<>(STATE_KEY_2, 9)));
    // NOTE: The future will still contain a reference to the underlying reader , thus not calling
    // assertNoReader(future).
  }

  @Test
  public void testReadValue() throws Exception {
    Future<Integer> future = underTest.valueFuture(STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addValuesToFetch(
                Windmill.TagValue.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .build());
    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addValues(
                Windmill.TagValue.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setValue(intValue(8)));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest.build()))
        .thenReturn(response.build());

    Integer result = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(result, Matchers.equalTo(8));
    assertNoReader(future);
  }

  @Test
  public void testReadWatermark() throws Exception {
    Future<Instant> future = underTest.watermarkFuture(STATE_KEY_1, STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addWatermarkHoldsToFetch(
                Windmill.WatermarkHold.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY));

    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addWatermarkHolds(
                Windmill.WatermarkHold.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addTimestamps(5000000)
                    .addTimestamps(6000000));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest.build()))
        .thenReturn(response.build());

    Instant result = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest.build());

    assertThat(result, Matchers.equalTo(new Instant(5000)));
    assertNoReader(future);
  }

  @Test
  public void testBatching() throws Exception {
    // Reads two bags and verifies that we batch them up correctly.
    Future<Instant> watermarkFuture = underTest.watermarkFuture(STATE_KEY_2, STATE_FAMILY);
    Future<Iterable<Integer>> bagFuture = underTest.bagFuture(STATE_KEY_1, STATE_FAMILY, INT_CODER);

    Mockito.verifyNoMoreInteractions(mockWindmill);

    ArgumentCaptor<Windmill.KeyedGetDataRequest> request =
        ArgumentCaptor.forClass(Windmill.KeyedGetDataRequest.class);

    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addWatermarkHolds(
                Windmill.WatermarkHold.newBuilder()
                    .setTag(STATE_KEY_2)
                    .setStateFamily(STATE_FAMILY)
                    .addTimestamps(5000000)
                    .addTimestamps(6000000))
            .addBags(
                Windmill.TagBag.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addValues(intData(5))
                    .addValues(intData(100)));

    Mockito.when(
            mockWindmill.getStateData(
                Mockito.eq(COMPUTATION), Mockito.isA(Windmill.KeyedGetDataRequest.class)))
        .thenReturn(response.build());
    Instant result = watermarkFuture.get();
    Mockito.verify(mockWindmill).getStateData(Mockito.eq(COMPUTATION), request.capture());

    // Verify the request looks right.
    KeyedGetDataRequest keyedRequest = request.getValue();
    assertThat(keyedRequest.getKey(), Matchers.equalTo(DATA_KEY));
    assertThat(keyedRequest.getWorkToken(), Matchers.equalTo(WORK_TOKEN));
    assertThat(keyedRequest.getBagsToFetchCount(), Matchers.equalTo(1));
    assertThat(keyedRequest.getBagsToFetch(0).getDeleteAll(), Matchers.equalTo(false));
    assertThat(keyedRequest.getBagsToFetch(0).getTag(), Matchers.equalTo(STATE_KEY_1));
    assertThat(keyedRequest.getWatermarkHoldsToFetchCount(), Matchers.equalTo(1));
    assertThat(keyedRequest.getWatermarkHoldsToFetch(0).getTag(), Matchers.equalTo(STATE_KEY_2));

    // Verify the values returned to the user.
    assertThat(result, Matchers.equalTo(new Instant(5000)));
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(bagFuture.get(), Matchers.contains(5, 100));
    Mockito.verifyNoMoreInteractions(mockWindmill);

    // And verify that getting a future again returns the already completed future.
    Future<Instant> watermarkFuture2 = underTest.watermarkFuture(STATE_KEY_2, STATE_FAMILY);
    assertTrue(watermarkFuture2.isDone());
    assertNoReader(watermarkFuture);
    assertNoReader(watermarkFuture2);
  }

  @Test
  public void testNoStateFamily() throws Exception {
    Future<Integer> future = underTest.valueFuture(STATE_KEY_1, "", INT_CODER);
    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataRequest.Builder expectedRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .setWorkToken(WORK_TOKEN)
            .addValuesToFetch(
                Windmill.TagValue.newBuilder().setTag(STATE_KEY_1).setStateFamily("").build());
    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addValues(
                Windmill.TagValue.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily("")
                    .setValue(intValue(8)));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest.build()))
        .thenReturn(response.build());

    Integer result = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(result, Matchers.equalTo(8));
    assertNoReader(future);
  }

  @Test
  public void testKeyTokenInvalid() throws Exception {
    // Reads two states and verifies that we batch them up correctly.
    Future<Instant> watermarkFuture = underTest.watermarkFuture(STATE_KEY_2, STATE_FAMILY);
    Future<Iterable<Integer>> bagFuture = underTest.bagFuture(STATE_KEY_1, STATE_FAMILY, INT_CODER);

    Mockito.verifyNoMoreInteractions(mockWindmill);

    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder().setKey(DATA_KEY).setFailed(true);

    Mockito.when(
            mockWindmill.getStateData(
                Mockito.eq(COMPUTATION), Mockito.isA(Windmill.KeyedGetDataRequest.class)))
        .thenReturn(response.build());

    try {
      watermarkFuture.get();
      fail("Expected KeyTokenInvalidException");
    } catch (Exception e) {
      assertTrue(KeyTokenInvalidException.isKeyTokenInvalidException(e));
    }

    try {
      bagFuture.get();
      fail("Expected KeyTokenInvalidException");
    } catch (Exception e) {
      assertTrue(KeyTokenInvalidException.isKeyTokenInvalidException(e));
    }
  }

  @Test
  public void testBatchingReadException() throws Exception {
    // Reads two states and verifies that we batch them up correctly and propagate the read
    // exception to both, not just the issuing future.
    Future<Instant> watermarkFuture = underTest.watermarkFuture(STATE_KEY_2, STATE_FAMILY);
    Future<Iterable<Integer>> bagFuture = underTest.bagFuture(STATE_KEY_1, STATE_FAMILY, INT_CODER);

    Mockito.verifyNoMoreInteractions(mockWindmill);

    RuntimeException expectedException = new RuntimeException("expected exception");

    Mockito.when(
            mockWindmill.getStateData(
                Mockito.eq(COMPUTATION), Mockito.isA(Windmill.KeyedGetDataRequest.class)))
        .thenThrow(expectedException);

    try {
      watermarkFuture.get();
      fail("Expected RuntimeException");
    } catch (Exception e) {
      assertThat(e.toString(), Matchers.containsString("expected exception"));
    }

    try {
      bagFuture.get();
      fail("Expected RuntimeException");
    } catch (Exception e) {
      assertThat(e.toString(), Matchers.containsString("expected exception"));
    }
  }

  @Test
  public void testBatchingCoderExceptions() throws Exception {
    // Read a batch of states with coder errors and verify it only affects the
    // relevant futures.
    Future<Integer> valueFuture = underTest.valueFuture(STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Future<Iterable<Integer>> bagFuture = underTest.bagFuture(STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Future<Iterable<Map.Entry<ByteString, Integer>>> valuePrefixFuture =
        underTest.valuePrefixFuture(STATE_KEY_PREFIX, STATE_FAMILY, INT_CODER);
    long beginning = SortedListRange.getDefaultInstance().getStart();
    long end = SortedListRange.getDefaultInstance().getLimit();
    Future<Iterable<TimestampedValue<Integer>>> orderedListFuture =
        underTest.orderedListFuture(
            Range.closedOpen(beginning, end), STATE_KEY_1, STATE_FAMILY, INT_CODER);
    // This should be the final part of the response, and we should process it successfully.
    Future<Instant> watermarkFuture = underTest.watermarkFuture(STATE_KEY_1, STATE_FAMILY);

    Mockito.verifyNoMoreInteractions(mockWindmill);

    ByteString invalidByteString = ByteString.copyFrom(BaseEncoding.base16().decode("FFFF"));
    Windmill.Value invalidValue = intValue(0).toBuilder().setData(invalidByteString).build();

    Windmill.KeyedGetDataRequest.Builder expectedRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(DATA_KEY)
            .setShardingKey(SHARDING_KEY)
            .setWorkToken(WORK_TOKEN)
            .setMaxBytes(WindmillStateReader.MAX_KEY_BYTES)
            .addValuesToFetch(
                Windmill.TagValue.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .build())
            .addBagsToFetch(
                Windmill.TagBag.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchMaxBytes(WindmillStateReader.INITIAL_MAX_BAG_BYTES))
            .addTagValuePrefixesToFetch(
                Windmill.TagValuePrefixRequest.newBuilder()
                    .setTagPrefix(STATE_KEY_PREFIX)
                    .setStateFamily(STATE_FAMILY)
                    .setFetchMaxBytes(WindmillStateReader.MAX_TAG_VALUE_PREFIX_BYTES))
            .addSortedListsToFetch(
                Windmill.TagSortedListFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(beginning).setLimit(end))
                    .setFetchMaxBytes(WindmillStateReader.MAX_ORDERED_LIST_BYTES))
            .addWatermarkHoldsToFetch(
                Windmill.WatermarkHold.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY));

    Windmill.KeyedGetDataResponse.Builder response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addValues(
                Windmill.TagValue.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .setValue(invalidValue))
            .addBags(
                Windmill.TagBag.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addValues(invalidByteString))
            .addTagValuePrefixes(
                Windmill.TagValuePrefixResponse.newBuilder()
                    .setTagPrefix(STATE_KEY_PREFIX)
                    .setStateFamily(STATE_FAMILY)
                    .addTagValues(
                        Windmill.TagValue.newBuilder()
                            .setTag(STATE_KEY_1)
                            .setStateFamily(STATE_FAMILY)
                            .setValue(invalidValue)))
            .addTagSortedLists(
                Windmill.TagSortedListFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        SortedListEntry.newBuilder()
                            .setValue(invalidByteString)
                            .setSortKey(5000)
                            .setId(5))
                    .addFetchRanges(SortedListRange.newBuilder().setStart(beginning).setLimit(end)))
            .addWatermarkHolds(
                Windmill.WatermarkHold.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addTimestamps(5000000)
                    .addTimestamps(6000000));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest.build()))
        .thenReturn(response.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);

    try {
      valueFuture.get();
      fail("Expected RuntimeException");
    } catch (Exception e) {
      assertThat(e.toString(), Matchers.containsString("Unable to decode value"));
    }

    try {
      bagFuture.get();
      fail("Expected RuntimeException");
    } catch (Exception e) {
      assertThat(e.toString(), Matchers.containsString("Error parsing bag"));
    }

    try {
      orderedListFuture.get();
      fail("Expected RuntimeException");
    } catch (Exception e) {
      assertThat(e.toString(), Matchers.containsString("Error parsing ordered list"));
    }

    try {
      valuePrefixFuture.get();
      fail("Expected RuntimeException");
    } catch (Exception e) {
      assertThat(e.toString(), Matchers.containsString("Error parsing tag value prefix"));
    }

    assertThat(watermarkFuture.get(), Matchers.equalTo(new Instant(5000)));
  }

  /**
   * Tests that multiple reads for the same tag in the same batch are cached. We can't compare the
   * futures since we've wrapped the delegate aronud them, so we just verify there is only one
   * queued lookup.
   */
  @Test
  public void testCachingWithinBatch() throws Exception {
    underTest.watermarkFuture(STATE_KEY_1, STATE_FAMILY);
    underTest.watermarkFuture(STATE_KEY_1, STATE_FAMILY);
    assertEquals(1, underTest.pendingLookups.size());
  }
}
