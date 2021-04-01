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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.SortedListEntry;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.SortedListRange;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString.Output;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Range;
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

/** Tests for {@link WindmillStateReader}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "FutureReturnValueIgnored",
})
public class WindmillStateReaderTest {
  private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
  private static final VarIntCoder INT_CODER = VarIntCoder.of();

  private static final String COMPUTATION = "computation";
  private static final ByteString DATA_KEY = ByteString.copyFromUtf8("DATA_KEY");
  private static final long SHARDING_KEY = 17L;
  private static final long WORK_TOKEN = 5043L;
  private static final long CONT_POSITION = 1391631351L;

  private static final ByteString STATE_KEY_PREFIX = ByteString.copyFromUtf8("key");
  private static final ByteString STATE_KEY_1 = ByteString.copyFromUtf8("key1");
  private static final ByteString STATE_KEY_2 = ByteString.copyFromUtf8("key2");
  private static final String STATE_FAMILY = "family";

  private static void assertNoReader(Object obj) throws Exception {
    WindmillStateTestUtils.assertNoReference(obj, WindmillStateReader.class);
  }

  @Mock private MetricTrackingWindmillServerStub mockWindmill;

  private WindmillStateReader underTest;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    underTest =
        new WindmillStateReader(mockWindmill, COMPUTATION, DATA_KEY, SHARDING_KEY, WORK_TOKEN);
  }

  private Windmill.Value intValue(int value) throws IOException {
    return Windmill.Value.newBuilder()
        .setData(intData(value))
        .setTimestamp(
            WindmillTimeUtils.harnessToWindmillTimestamp(BoundedWindow.TIMESTAMP_MAX_VALUE))
        .build();
  }

  private ByteString intData(int value) throws IOException {
    Output output = ByteString.newOutput();
    INT_CODER.encode(value, output, Coder.Context.OUTER);
    return output.toByteString();
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
    // NOTE: The future will still contain a reference to the underlying reader.
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
    Future<Iterable<TimestampedValue<Integer>>> future2 =
        underTest.orderedListFuture(Range.closedOpen(5L, 6L), STATE_KEY_1, STATE_FAMILY, INT_CODER);
    Future<Iterable<TimestampedValue<Integer>>> future3 =
        underTest.orderedListFuture(
            Range.closedOpen(6L, 10L), STATE_KEY_1, STATE_FAMILY, INT_CODER);
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
                    .addFetchRanges(SortedListRange.newBuilder().setStart(0).setLimit(5))
                    .setFetchMaxBytes(WindmillStateReader.MAX_ORDERED_LIST_BYTES))
            .addSortedListsToFetch(
                Windmill.TagSortedListFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(5).setLimit(6))
                    .setFetchMaxBytes(WindmillStateReader.MAX_ORDERED_LIST_BYTES))
            .addSortedListsToFetch(
                Windmill.TagSortedListFetchRequest.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addFetchRanges(SortedListRange.newBuilder().setStart(6).setLimit(10))
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
                    .addFetchRanges(SortedListRange.newBuilder().setStart(0).setLimit(5)))
            .addTagSortedLists(
                Windmill.TagSortedListFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(6)).setSortKey(6000).setId(5))
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(7)).setSortKey(7000).setId(7))
                    .addFetchRanges(SortedListRange.newBuilder().setStart(5).setLimit(6)))
            .addTagSortedLists(
                Windmill.TagSortedListFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(8)).setSortKey(8000).setId(8))
                    .addFetchRanges(SortedListRange.newBuilder().setStart(6).setLimit(10)));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest.build()))
        .thenReturn(response.build());

    {
      Iterable<TimestampedValue<Integer>> results = future1.get();
      Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest.build());
      for (TimestampedValue<Integer> unused : results) {
        // Iterate over the results to force loading all the pages.
      }
      Mockito.verifyNoMoreInteractions(mockWindmill);
      assertThat(results, Matchers.contains(TimestampedValue.of(5, Instant.ofEpochMilli(5))));
      assertNoReader(future1);
    }

    {
      Iterable<TimestampedValue<Integer>> results = future2.get();
      Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest.build());
      for (TimestampedValue<Integer> unused : results) {
        // Iterate over the results to force loading all the pages.
      }
      Mockito.verifyNoMoreInteractions(mockWindmill);
      assertThat(
          results,
          Matchers.contains(
              TimestampedValue.of(6, Instant.ofEpochMilli(6)),
              TimestampedValue.of(7, Instant.ofEpochMilli(7))));
      assertNoReader(future2);
    }

    {
      Iterable<TimestampedValue<Integer>> results = future3.get();
      Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest.build());
      for (TimestampedValue<Integer> unused : results) {
        // Iterate over the results to force loading all the pages.
      }
      Mockito.verifyNoMoreInteractions(mockWindmill);
      assertThat(results, Matchers.contains(TimestampedValue.of(8, Instant.ofEpochMilli(8))));
      assertNoReader(future3);
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

    final ByteString CONT = ByteString.copyFrom("CONTINUATION", Charsets.UTF_8);
    Windmill.KeyedGetDataResponse.Builder response1 =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setKey(DATA_KEY)
            .addTagSortedLists(
                Windmill.TagSortedListFetchResponse.newBuilder()
                    .setTag(STATE_KEY_1)
                    .setStateFamily(STATE_FAMILY)
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(5)).setSortKey(5000).setId(5))
                    .setContinuationPosition(CONT)
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
                    .setRequestPosition(CONT)
                    .setFetchMaxBytes(WindmillStateReader.MAX_ORDERED_LIST_BYTES));

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
                    .addEntries(
                        SortedListEntry.newBuilder().setValue(intData(8)).setSortKey(8000).setId(8))
                    .addFetchRanges(SortedListRange.newBuilder().setStart(beginning).setLimit(end))
                    .setRequestPosition(CONT));

    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest1.build()))
        .thenReturn(response1.build());
    Mockito.when(mockWindmill.getStateData(COMPUTATION, expectedRequest2.build()))
        .thenReturn(response2.build());

    Iterable<TimestampedValue<Integer>> results = future.get();
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest1.build());
    for (TimestampedValue<Integer> unused : results) {
      // Iterate over the results to force loading all the pages.
    }
    Mockito.verify(mockWindmill).getStateData(COMPUTATION, expectedRequest2.build());
    Mockito.verifyNoMoreInteractions(mockWindmill);

    assertThat(
        results,
        Matchers.contains(
            TimestampedValue.of(5, Instant.ofEpochMilli(5)),
            TimestampedValue.of(6, Instant.ofEpochMilli(6)),
            TimestampedValue.of(7, Instant.ofEpochMilli(7)),
            TimestampedValue.of(8, Instant.ofEpochMilli(8))));
    // NOTE: The future will still contain a reference to the underlying reader.
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

    final ByteString CONT = ByteString.copyFrom("CONTINUATION", Charsets.UTF_8);
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
    // NOTE: The future will still contain a reference to the underlying reader.
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
    // Reads two bags and verifies that we batch them up correctly.
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
