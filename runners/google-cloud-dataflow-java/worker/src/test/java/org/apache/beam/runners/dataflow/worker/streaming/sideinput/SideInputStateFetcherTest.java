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
package org.apache.beam.runners.dataflow.worker.streaming.sideinput;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.worker.MetricTrackingWindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link SideInputStateFetcher}. */
// TODO: Add tests with different encoded windows to verify version is correctly plumbed.
@SuppressWarnings("deprecation")
@RunWith(JUnit4.class)
public class SideInputStateFetcherTest {
  private static final String STATE_FAMILY = "state";

  @Mock private MetricTrackingWindmillServerStub server;

  @Mock private Supplier<Closeable> readStateSupplier;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testFetchGlobalDataBasic() throws Exception {
    SideInputStateFetcher fetcher = new SideInputStateFetcher(server);

    ByteStringOutputStream stream = new ByteStringOutputStream();
    ListCoder.of(StringUtf8Coder.of())
        .encode(Collections.singletonList("data"), stream, Coder.Context.OUTER);
    ByteString encodedIterable = stream.toByteString();

    PCollectionView<String> view =
        TestPipeline.create().apply(Create.empty(StringUtf8Coder.of())).apply(View.asSingleton());

    String tag = view.getTagInternal().getId();

    // Test three calls in a row. First, data is not ready, then data is ready,
    // then the data is already cached.
    when(server.getSideInputData(any(Windmill.GlobalDataRequest.class)))
        .thenReturn(
            buildGlobalDataResponse(tag, false, null),
            buildGlobalDataResponse(tag, true, encodedIterable));

    assertFalse(
        fetcher
            .fetchSideInput(
                view,
                GlobalWindow.INSTANCE,
                STATE_FAMILY,
                SideInputState.UNKNOWN,
                readStateSupplier)
            .isReady());

    assertFalse(
        fetcher
            .fetchSideInput(
                view,
                GlobalWindow.INSTANCE,
                STATE_FAMILY,
                SideInputState.UNKNOWN,
                readStateSupplier)
            .isReady());

    assertEquals(
        "data",
        fetcher
            .fetchSideInput(
                view,
                GlobalWindow.INSTANCE,
                STATE_FAMILY,
                SideInputState.KNOWN_READY,
                readStateSupplier)
            .value()
            .orElse(null));
    assertEquals(
        "data",
        fetcher
            .fetchSideInput(
                view,
                GlobalWindow.INSTANCE,
                STATE_FAMILY,
                SideInputState.KNOWN_READY,
                readStateSupplier)
            .value()
            .orElse(null));

    verify(server, times(2)).getSideInputData(buildGlobalDataRequest(tag));
    verifyNoMoreInteractions(server);
  }

  @Test
  public void testFetchGlobalDataNull() throws Exception {
    SideInputStateFetcher fetcher = new SideInputStateFetcher(server);

    ByteStringOutputStream stream = new ByteStringOutputStream();
    ListCoder.of(VoidCoder.of())
        .encode(Collections.singletonList(null), stream, Coder.Context.OUTER);
    ByteString encodedIterable = stream.toByteString();

    PCollectionView<Void> view =
        TestPipeline.create().apply(Create.empty(VoidCoder.of())).apply(View.asSingleton());

    String tag = view.getTagInternal().getId();

    // Test three calls in a row. First, data is not ready, then data is ready,
    // then the data is already cached.
    when(server.getSideInputData(any(Windmill.GlobalDataRequest.class)))
        .thenReturn(
            buildGlobalDataResponse(tag, false, null),
            buildGlobalDataResponse(tag, true, encodedIterable));

    assertFalse(
        fetcher
            .fetchSideInput(
                view,
                GlobalWindow.INSTANCE,
                STATE_FAMILY,
                SideInputState.UNKNOWN,
                readStateSupplier)
            .isReady());
    assertFalse(
        fetcher
            .fetchSideInput(
                view,
                GlobalWindow.INSTANCE,
                STATE_FAMILY,
                SideInputState.UNKNOWN,
                readStateSupplier)
            .isReady());
    assertNull(
        fetcher
            .fetchSideInput(
                view,
                GlobalWindow.INSTANCE,
                STATE_FAMILY,
                SideInputState.KNOWN_READY,
                readStateSupplier)
            .value()
            .orElse(null));
    assertNull(
        fetcher
            .fetchSideInput(
                view,
                GlobalWindow.INSTANCE,
                STATE_FAMILY,
                SideInputState.KNOWN_READY,
                readStateSupplier)
            .value()
            .orElse(null));

    verify(server, times(2)).getSideInputData(buildGlobalDataRequest(tag));
    verifyNoMoreInteractions(server);
  }

  @Test
  public void testFetchGlobalDataCacheOverflow() throws Exception {
    Coder<List<String>> coder = ListCoder.of(StringUtf8Coder.of());

    ByteStringOutputStream stream = new ByteStringOutputStream();
    coder.encode(Collections.singletonList("data1"), stream, Coder.Context.OUTER);
    ByteString encodedIterable1 = stream.toByteStringAndReset();
    coder.encode(Collections.singletonList("data2"), stream, Coder.Context.OUTER);
    ByteString encodedIterable2 = stream.toByteString();

    Cache<SideInputCache.Key<?>, SideInput<?>> cache = CacheBuilder.newBuilder().build();

    SideInputStateFetcher fetcher = new SideInputStateFetcher(server, new SideInputCache(cache));

    PCollectionView<String> view1 =
        TestPipeline.create().apply(Create.empty(StringUtf8Coder.of())).apply(View.asSingleton());

    PCollectionView<String> view2 =
        TestPipeline.create().apply(Create.empty(StringUtf8Coder.of())).apply(View.asSingleton());

    String tag1 = view1.getTagInternal().getId();
    String tag2 = view2.getTagInternal().getId();

    // Test four calls in a row. First, fetch view1, then view2 (which evicts view1 from the cache),
    // then view 1 again twice.
    when(server.getSideInputData(any(Windmill.GlobalDataRequest.class)))
        .thenReturn(
            buildGlobalDataResponse(tag1, true, encodedIterable1),
            buildGlobalDataResponse(tag2, true, encodedIterable2),
            buildGlobalDataResponse(tag1, true, encodedIterable1));

    assertEquals(
        "data1",
        fetcher
            .fetchSideInput(
                view1,
                GlobalWindow.INSTANCE,
                STATE_FAMILY,
                SideInputState.UNKNOWN,
                readStateSupplier)
            .value()
            .orElse(null));
    assertEquals(
        "data2",
        fetcher
            .fetchSideInput(
                view2,
                GlobalWindow.INSTANCE,
                STATE_FAMILY,
                SideInputState.UNKNOWN,
                readStateSupplier)
            .value()
            .orElse(null));
    cache.invalidateAll();
    assertEquals(
        "data1",
        fetcher
            .fetchSideInput(
                view1,
                GlobalWindow.INSTANCE,
                STATE_FAMILY,
                SideInputState.UNKNOWN,
                readStateSupplier)
            .value()
            .orElse(null));
    assertEquals(
        "data1",
        fetcher
            .fetchSideInput(
                view1,
                GlobalWindow.INSTANCE,
                STATE_FAMILY,
                SideInputState.UNKNOWN,
                readStateSupplier)
            .value()
            .orElse(null));

    ArgumentCaptor<Windmill.GlobalDataRequest> captor =
        ArgumentCaptor.forClass(Windmill.GlobalDataRequest.class);

    verify(server, times(3)).getSideInputData(captor.capture());
    verifyNoMoreInteractions(server);

    assertThat(
        captor.getAllValues(),
        contains(
            buildGlobalDataRequest(tag1),
            buildGlobalDataRequest(tag2),
            buildGlobalDataRequest(tag1)));
  }

  @Test
  public void testEmptyFetchGlobalData() throws Exception {
    SideInputStateFetcher fetcher = new SideInputStateFetcher(server);

    ByteString encodedIterable = ByteString.EMPTY;

    PCollectionView<Long> view =
        TestPipeline.create()
            .apply(Create.empty(VarLongCoder.of()))
            .apply(Sum.longsGlobally().asSingletonView());

    String tag = view.getTagInternal().getId();

    // Test three calls in a row. First, data is not ready, then data is ready,
    // then the data is already cached.
    when(server.getSideInputData(any(Windmill.GlobalDataRequest.class)))
        .thenReturn(buildGlobalDataResponse(tag, true, encodedIterable));

    assertEquals(
        0L,
        (long)
            fetcher
                .fetchSideInput(
                    view,
                    GlobalWindow.INSTANCE,
                    STATE_FAMILY,
                    SideInputState.UNKNOWN,
                    readStateSupplier)
                .value()
                .orElse(null));

    verify(server).getSideInputData(buildGlobalDataRequest(tag));
    verifyNoMoreInteractions(server);
  }

  private static Windmill.GlobalData buildGlobalDataResponse(
      String tag, boolean isReady, ByteString data) {
    Windmill.GlobalData.Builder builder =
        Windmill.GlobalData.newBuilder()
            .setDataId(
                Windmill.GlobalDataId.newBuilder()
                    .setTag(tag)
                    .setVersion(ByteString.EMPTY)
                    .build());

    if (isReady) {
      builder.setIsReady(true).setData(data);
    } else {
      builder.setIsReady(false);
    }
    return builder.build();
  }

  private static Windmill.GlobalDataRequest buildGlobalDataRequest(String tag, ByteString version) {
    Windmill.GlobalDataId id =
        Windmill.GlobalDataId.newBuilder().setTag(tag).setVersion(version).build();

    return Windmill.GlobalDataRequest.newBuilder()
        .setDataId(id)
        .setStateFamily(STATE_FAMILY)
        .setExistenceWatermarkDeadline(
            TimeUnit.MILLISECONDS.toMicros(GlobalWindow.INSTANCE.maxTimestamp().getMillis()))
        .build();
  }

  private static Windmill.GlobalDataRequest buildGlobalDataRequest(String tag) {
    return buildGlobalDataRequest(tag, ByteString.EMPTY);
  }
}
