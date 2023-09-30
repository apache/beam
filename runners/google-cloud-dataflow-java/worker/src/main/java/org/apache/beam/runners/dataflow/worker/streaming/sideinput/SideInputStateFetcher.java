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

import static org.apache.beam.sdk.transforms.Materializations.ITERABLE_MATERIALIZATION_URN;
import static org.apache.beam.sdk.transforms.Materializations.MULTIMAP_MATERIALIZATION_URN;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.dataflow.worker.MetricTrackingWindmillServerStub;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class responsible for fetching state from the windmill server. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@NotThreadSafe
public class SideInputStateFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(SideInputStateFetcher.class);

  private static final Set<String> SUPPORTED_MATERIALIZATIONS =
      ImmutableSet.of(ITERABLE_MATERIALIZATION_URN, MULTIMAP_MATERIALIZATION_URN);

  private final SideInputCache sideInputCache;
  private final MetricTrackingWindmillServerStub server;
  private long bytesRead = 0L;

  public SideInputStateFetcher(MetricTrackingWindmillServerStub server) {
    this(server, SideInputCache.create());
  }

  SideInputStateFetcher(MetricTrackingWindmillServerStub server, SideInputCache sideInputCache) {
    this.server = server;
    this.sideInputCache = sideInputCache;
  }

  @SuppressWarnings("deprecation")
  private static Iterable<?> decodeRawData(Coder<?> viewInternalCoder, GlobalData data)
      throws IOException {
    return !data.getData().isEmpty()
        ? IterableCoder.of(viewInternalCoder).decode(data.getData().newInput(), Coder.Context.OUTER)
        : Collections.emptyList();
  }

  /** Returns a view of the underlying cache that keeps track of bytes read separately. */
  public SideInputStateFetcher byteTrackingView() {
    return new SideInputStateFetcher(server, sideInputCache);
  }

  public long getBytesRead() {
    return bytesRead;
  }

  /**
   * Fetch the given side input, storing it in a process-level cache.
   *
   * <p>If state is KNOWN_READY, attempt to fetch the data regardless of whether a not-ready entry
   * was cached.
   *
   * <p>Returns {@literal null} if the side input was not ready, {@literal Optional.absent()} if the
   * side input was null, and {@literal Optional.present(...)} if the side input was non-null.
   */
  @SuppressWarnings("deprecation")
  public <T> SideInput<T> fetchSideInput(
      PCollectionView<T> view,
      BoundedWindow sideWindow,
      String stateFamily,
      SideInputState state,
      Supplier<Closeable> scopedReadStateSupplier) {
    Callable<SideInput<T>> loadSideInputFromWindmill =
        () -> loadSideInputFromWindmill(view, sideWindow, stateFamily, scopedReadStateSupplier);

    SideInputCache.Key sideInputCacheKey =
        SideInputCache.Key.create(view.getTagInternal(), sideWindow);

    try {
      if (state == SideInputState.KNOWN_READY) {
        Optional<SideInput<T>> existingCacheEntry = sideInputCache.get(sideInputCacheKey);
        if (!existingCacheEntry.isPresent()) {
          return sideInputCache.getOrLoad(sideInputCacheKey, loadSideInputFromWindmill);
        }

        if (!existingCacheEntry.get().isReady()) {
          return sideInputCache.invalidateThenLoadNewEntry(
              sideInputCacheKey, loadSideInputFromWindmill);
        }

        return existingCacheEntry.get();
      }

      return sideInputCache.getOrLoad(sideInputCacheKey, loadSideInputFromWindmill);
    } catch (Exception e) {
      LOG.error("Fetch failed: ", e);
      throw new RuntimeException("Exception while fetching side input: ", e);
    }
  }

  @SuppressWarnings({"deprecation", "unchecked"})
  private <T, SideWindowT extends BoundedWindow> GlobalData fetchGlobalDataFromWindmill(
      PCollectionView<T> view,
      SideWindowT sideWindow,
      String stateFamily,
      Supplier<Closeable> scopedReadStateSupplier)
      throws IOException {
    WindowingStrategy<?, SideWindowT> sideWindowStrategy =
        (WindowingStrategy<?, SideWindowT>) view.getWindowingStrategyInternal();

    Coder<SideWindowT> windowCoder = sideWindowStrategy.getWindowFn().windowCoder();

    ByteStringOutputStream windowStream = new ByteStringOutputStream();
    windowCoder.encode(sideWindow, windowStream, Coder.Context.OUTER);

    Windmill.GlobalDataRequest request =
        Windmill.GlobalDataRequest.newBuilder()
            .setDataId(
                Windmill.GlobalDataId.newBuilder()
                    .setTag(view.getTagInternal().getId())
                    .setVersion(windowStream.toByteString())
                    .build())
            .setStateFamily(stateFamily)
            .setExistenceWatermarkDeadline(
                WindmillTimeUtils.harnessToWindmillTimestamp(
                    sideWindowStrategy.getTrigger().getWatermarkThatGuaranteesFiring(sideWindow)))
            .build();

    try (Closeable ignored = scopedReadStateSupplier.get()) {
      return server.getSideInputData(request);
    }
  }

  @SuppressWarnings("deprecation")
  private <T> SideInput<T> loadSideInputFromWindmill(
      PCollectionView<T> view,
      BoundedWindow sideWindow,
      String stateFamily,
      Supplier<Closeable> scopedReadStateSupplier)
      throws IOException {
    checkState(
        SUPPORTED_MATERIALIZATIONS.contains(view.getViewFn().getMaterialization().getUrn()),
        "Only materialization's of type %s supported, received %s",
        SUPPORTED_MATERIALIZATIONS,
        view.getViewFn().getMaterialization().getUrn());

    GlobalData data =
        fetchGlobalDataFromWindmill(view, sideWindow, stateFamily, scopedReadStateSupplier);
    bytesRead += data.getSerializedSize();
    return data.getIsReady() ? createSideInputCacheEntry(view, data) : SideInput.notReady();
  }

  @SuppressWarnings({"deprecation", "unchecked"})
  private <T> SideInput<T> createSideInputCacheEntry(PCollectionView<T> view, GlobalData data)
      throws IOException {
    Iterable<?> rawData = decodeRawData(view.getCoderInternal(), data);
    switch (view.getViewFn().getMaterialization().getUrn()) {
      case ITERABLE_MATERIALIZATION_URN:
        {
          ViewFn<IterableView, T> viewFn = (ViewFn<IterableView, T>) view.getViewFn();
          return SideInput.ready(viewFn.apply(() -> rawData), data.getData().size());
        }
      case MULTIMAP_MATERIALIZATION_URN:
        {
          ViewFn<MultimapView, T> viewFn = (ViewFn<MultimapView, T>) view.getViewFn();
          Coder<?> keyCoder = ((KvCoder<?, ?>) view.getCoderInternal()).getKeyCoder();
          return SideInput.ready(
              viewFn.apply(
                  InMemoryMultimapSideInputView.fromIterable(keyCoder, (Iterable) rawData)),
              data.getData().size());
        }
      default:
        throw new IllegalStateException(
            String.format(
                "Unknown side input materialization format requested '%s'",
                view.getViewFn().getMaterialization().getUrn()));
    }
  }
}
