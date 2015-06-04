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

package com.google.cloud.dataflow.sdk.util;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.WindmillServerStub;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.util.StateFetcher.SideInputState;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Unit tests for {@link StateFetcher}. */
@RunWith(JUnit4.class)
public class StateFetcherTest {

  @Mock
  WindmillServerStub server;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testFetch() throws Exception {
    StateFetcher fetcher = new StateFetcher(server);

    when(server.getData(any(Windmill.GetDataRequest.class))).thenReturn(
        Windmill.GetDataResponse.newBuilder()
        .addData(Windmill.ComputationGetDataResponse.newBuilder()
            .setComputationId("computation")
            .addData(Windmill.KeyedGetDataResponse.newBuilder()
                .setKey(ByteString.copyFromUtf8("key"))
                .addValues(Windmill.TagValue.newBuilder()
                    .setTag(ByteString.copyFromUtf8("p:tag1"))
                    .setValue(Windmill.Value.newBuilder()
                        .setTimestamp(0)
                        .setData(ByteString.copyFromUtf8("data1"))
                        .build())
                    .build())
                .addValues(Windmill.TagValue.newBuilder()
                    .setTag(ByteString.copyFromUtf8("p:tag2"))
                    .setValue(Windmill.Value.newBuilder()
                        .setTimestamp(0)
                        .setData(ByteString.copyFromUtf8("data2"))
                        .build())
                    .build())
                .build())
            .build())
        .build());

    Map<CodedTupleTag<?>, Optional<?>> data =
        fetcher.fetch("computation", ByteString.copyFromUtf8("key"), 17L, "p:",
            Arrays.asList(
                CodedTupleTag.of("tag1", StringUtf8Coder.of()),
                CodedTupleTag.of("tag2", StringUtf8Coder.of())));

    verify(server).getData(
        Windmill.GetDataRequest.newBuilder()
        .addRequests(Windmill.ComputationGetDataRequest.newBuilder()
            .setComputationId("computation")
            .addRequests(Windmill.KeyedGetDataRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("key"))
                .setWorkToken(17L)
                .addValuesToFetch(Windmill.TagValue.newBuilder()
                    .setTag(ByteString.copyFromUtf8("p:tag1"))
                    .build())
                .addValuesToFetch(Windmill.TagValue.newBuilder()
                    .setTag(ByteString.copyFromUtf8("p:tag2"))
                    .build())
                .build())
            .build())
        .build());

    assertEquals(2, data.size());
    assertEquals("data1", data.get(CodedTupleTag.of("tag1", StringUtf8Coder.of())).get());
    assertEquals("data2", data.get(CodedTupleTag.of("tag2", StringUtf8Coder.of())).get());
  }

  @Test
  public void testFetchList() throws Exception {
    StateFetcher fetcher = new StateFetcher(server);

    when(server.getData(any(Windmill.GetDataRequest.class))).thenReturn(
        Windmill.GetDataResponse.newBuilder()
        .addData(Windmill.ComputationGetDataResponse.newBuilder()
            .setComputationId("computation")
            .addData(Windmill.KeyedGetDataResponse.newBuilder()
                .setKey(ByteString.copyFromUtf8("key"))
                .addLists(Windmill.TagList.newBuilder()
                    .setTag(ByteString.copyFromUtf8("p:tag1"))
                    .addValues(Windmill.Value.newBuilder()
                        .setTimestamp(0)
                        .setData(ByteString.copyFromUtf8("\000data1"))
                        .build())
                    .addValues(Windmill.Value.newBuilder()
                        .setTimestamp(1000)
                        .setData(ByteString.copyFromUtf8("\000data2"))
                        .build())
                    .build())
                .build())
            .build())
        .build());

    CodedTupleTag<String> tag = CodedTupleTag.of("tag1", StringUtf8Coder.of());
    @SuppressWarnings("unchecked")
    List<String> data =
        (List<String>) fetcher.fetchList("computation", ByteString.copyFromUtf8("key"), 17L, "p:",
        Arrays.asList(tag)).get(tag);

    verify(server).getData(
        Windmill.GetDataRequest.newBuilder()
        .addRequests(Windmill.ComputationGetDataRequest.newBuilder()
            .setComputationId("computation")
            .addRequests(Windmill.KeyedGetDataRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("key"))
                .setWorkToken(17L)
                .addListsToFetch(Windmill.TagList.newBuilder()
                    .setTag(ByteString.copyFromUtf8("p:tag1"))
                    .setEndTimestamp(Long.MAX_VALUE)
                    .build())
                .build())
            .build())
        .build());

    assertThat(data, contains("data1", "data2"));
  }

  @Test
  public void testFetchGlobalDataBasic() throws Exception {
    StateFetcher fetcher = new StateFetcher(server);

    ByteString.Output stream = ByteString.newOutput();
    ListCoder.of(WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE))
        .encode(Arrays.asList(WindowedValue.valueInGlobalWindow("data")),
            stream, Coder.Context.OUTER);
    ByteString encodedIterable = stream.toByteString();

    PCollectionView<String> view =
        TestPipeline.create().apply(Create.<String>of()
            .withCoder(StringUtf8Coder.of())).apply(View.<String>asSingleton());

    String tag = view.getTagInternal().getId();

    // Test three calls in a row. First, data is not ready, then data is ready,
    // then the data is already cached.
    when(server.getData(any(Windmill.GetDataRequest.class))).thenReturn(
        buildGlobalDataResponse(tag, ByteString.EMPTY, false, null),
        buildGlobalDataResponse(tag, ByteString.EMPTY, true, encodedIterable));

    assertEquals(null, fetcher.fetchSideInput(view, GlobalWindow.INSTANCE, SideInputState.UNKNOWN));
    assertEquals(null, fetcher.fetchSideInput(view, GlobalWindow.INSTANCE, SideInputState.UNKNOWN));
    assertEquals("data",
        fetcher.fetchSideInput(view, GlobalWindow.INSTANCE, SideInputState.KNOWN_READY));
    assertEquals("data",
        fetcher.fetchSideInput(view, GlobalWindow.INSTANCE, SideInputState.KNOWN_READY));

    verify(server, times(2)).getData(buildGlobalDataRequest(tag, ByteString.EMPTY));
    verifyNoMoreInteractions(server);
  }

  @Test
  public void testFetchGlobalDataCacheOverflow() throws Exception {
    Coder<List<WindowedValue<String>>> coder =
        ListCoder.of(WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE));

    ByteString.Output stream = ByteString.newOutput();
    coder.encode(Arrays.asList(WindowedValue.valueInGlobalWindow("data1")),
        stream, Coder.Context.OUTER);
    ByteString encodedIterable1 = stream.toByteString();
    stream = ByteString.newOutput();
    coder.encode(Arrays.asList(WindowedValue.valueInGlobalWindow("data2")),
        stream, Coder.Context.OUTER);
    ByteString encodedIterable2 = stream.toByteString();

    Cache<StateFetcher.SideInputId, StateFetcher.SideInputCacheEntry> cache =
        CacheBuilder.newBuilder().build();

    StateFetcher fetcher = new StateFetcher(server, cache);

    PCollectionView<String> view1 =
        TestPipeline.create().apply(Create.<String>of()
            .withCoder(StringUtf8Coder.of())).apply(View.<String>asSingleton());

    PCollectionView<String> view2 =
        TestPipeline.create().apply(Create.<String>of()
            .withCoder(StringUtf8Coder.of())).apply(View.<String>asSingleton());

    String tag1 = view1.getTagInternal().getId();
    String tag2 = view2.getTagInternal().getId();

    // Test four calls in a row. First, fetch view1, then view2 (which evicts view1 from the cache),
    // then view 1 again twice.
    when(server.getData(any(Windmill.GetDataRequest.class))).thenReturn(
        buildGlobalDataResponse(tag1, ByteString.EMPTY, true, encodedIterable1),
        buildGlobalDataResponse(tag2, ByteString.EMPTY, true, encodedIterable2),
        buildGlobalDataResponse(tag1, ByteString.EMPTY, true, encodedIterable1));

    assertEquals("data1",
        fetcher.fetchSideInput(view1, GlobalWindow.INSTANCE, SideInputState.UNKNOWN));
    assertEquals("data2",
        fetcher.fetchSideInput(view2, GlobalWindow.INSTANCE, SideInputState.UNKNOWN));
    cache.invalidateAll();
    assertEquals("data1",
        fetcher.fetchSideInput(view1, GlobalWindow.INSTANCE, SideInputState.UNKNOWN));
    assertEquals("data1",
        fetcher.fetchSideInput(view1, GlobalWindow.INSTANCE, SideInputState.UNKNOWN));

    ArgumentCaptor<Windmill.GetDataRequest> captor =
        ArgumentCaptor.forClass(Windmill.GetDataRequest.class);

    verify(server, times(3)).getData(captor.capture());
    verifyNoMoreInteractions(server);

    assertThat(captor.getAllValues(), contains(
        buildGlobalDataRequest(tag1, ByteString.EMPTY),
        buildGlobalDataRequest(tag2, ByteString.EMPTY),
        buildGlobalDataRequest(tag1, ByteString.EMPTY)));
  }

  @Test
  public void testEmptyFetchGlobalData() throws Exception {
    StateFetcher fetcher = new StateFetcher(server);

    ByteString encodedIterable = ByteString.EMPTY;

    PCollectionView<Long> view =
        TestPipeline.create().apply(Create.<Long>of()
            .withCoder(VarLongCoder.of())).apply(Sum.longsGlobally().asSingletonView());

    String tag = view.getTagInternal().getId();

    // Test three calls in a row. First, data is not ready, then data is ready,
    // then the data is already cached.
    when(server.getData(any(Windmill.GetDataRequest.class))).thenReturn(
        buildGlobalDataResponse(tag, ByteString.EMPTY, true, encodedIterable));

    assertEquals(0L,
        (long) fetcher.fetchSideInput(view, GlobalWindow.INSTANCE, SideInputState.UNKNOWN));

    verify(server).getData(buildGlobalDataRequest(tag, ByteString.EMPTY));
    verifyNoMoreInteractions(server);
  }

  private Windmill.GetDataResponse buildGlobalDataResponse(
      String tag, ByteString version, boolean isReady, ByteString data) {
    Windmill.GlobalData.Builder builder = Windmill.GlobalData.newBuilder()
        .setDataId(Windmill.GlobalDataId.newBuilder()
            .setTag(tag)
            .setVersion(version)
            .build());

    if (isReady) {
      builder.setIsReady(true).setData(data);
    } else {
      builder.setIsReady(false);
    }
    return Windmill.GetDataResponse.newBuilder()
        .addGlobalData(builder.build()).build();
  }

  private Windmill.GetDataRequest buildGlobalDataRequest(
      String tag, ByteString version) {
    Windmill.GlobalDataId id =
        Windmill.GlobalDataId.newBuilder().setTag(tag).setVersion(version).build();

    return Windmill.GetDataRequest.newBuilder()
        .addGlobalDataFetchRequests(
             Windmill.GlobalDataRequest.newBuilder()
                 .setDataId(id)
                 .setExistenceWatermarkDeadline(
                      TimeUnit.MILLISECONDS.toMicros(
                          GlobalWindow.INSTANCE.maxTimestamp().getMillis()))
                 .build())
        .addGlobalDataToFetch(id)
        .build();
  }
}
