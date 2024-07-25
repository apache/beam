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
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class responsible for fetching side input state from the streaming backend. */
@NotThreadSafe
@Internal
public class SideInputStateFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(SideInputStateFetcher.class);

  private static final Set<String> SUPPORTED_MATERIALIZATIONS =
      ImmutableSet.of(ITERABLE_MATERIALIZATION_URN, MULTIMAP_MATERIALIZATION_URN);

  private final SideInputCache sideInputCache;
  private final Function<GlobalDataRequest, GlobalData> fetchGlobalDataFn;
  private long bytesRead = 0L;

  SideInputStateFetcher(
      Function<GlobalDataRequest, GlobalData> fetchGlobalDataFn, SideInputCache sideInputCache) {
    this.fetchGlobalDataFn = fetchGlobalDataFn;
    this.sideInputCache = sideInputCache;
  }

  private static <T> Iterable<?> decodeRawData(PCollectionView<T> view, GlobalData data)
      throws IOException {
    return !data.getData().isEmpty()
        ? IterableCoder.of(getCoder(view)).decode(data.getData().newInput())
        : Collections.emptyList();
  }

  @SuppressWarnings({
    "deprecation" // Required as part of the SideInputCacheKey, and not exposed.
  })
  private static <T> TupleTag<?> getInternalTag(PCollectionView<T> view) {
    return view.getTagInternal();
  }

  @SuppressWarnings("deprecation")
  private static <T> ViewFn<?, T> getViewFn(PCollectionView<T> view) {
    return view.getViewFn();
  }

  @SuppressWarnings({
    "deprecation" // The view's internal coder is required to decode the raw data.
  })
  private static <T> Coder<?> getCoder(PCollectionView<T> view) {
    return view.getCoderInternal();
  }

  private static <T> SideInput<T> createSideInputCacheEntry(
      PCollectionView<T> view, GlobalData data) throws IOException {
    Iterable<?> rawData = decodeRawData(view, data);
    switch (getViewFn(view).getMaterialization().getUrn()) {
      case ITERABLE_MATERIALIZATION_URN:
        {
          @SuppressWarnings({
            "unchecked", // ITERABLE_MATERIALIZATION_URN has ViewFn<IterableView, T>.
            "rawtypes" //  TODO(https://github.com/apache/beam/issues/20447)
          })
          ViewFn<IterableView, T> viewFn = (ViewFn<IterableView, T>) getViewFn(view);
          return SideInput.ready(viewFn.apply(() -> rawData), data.getData().size());
        }
      case MULTIMAP_MATERIALIZATION_URN:
        {
          @SuppressWarnings({
            "unchecked", // MULTIMAP_MATERIALIZATION_URN has ViewFn<MultimapView, T>.
            "rawtypes" //  TODO(https://github.com/apache/beam/issues/20447)
          })
          ViewFn<MultimapView, T> viewFn = (ViewFn<MultimapView, T>) getViewFn(view);
          Coder<?> keyCoder = ((KvCoder<?, ?>) getCoder(view)).getKeyCoder();

          @SuppressWarnings({
            "unchecked", // Safe since multimap rawData is of type Iterable<KV<K, V>>
            "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
          })
          T multimapSideInputValue =
              viewFn.apply(
                  InMemoryMultimapSideInputView.fromIterable(keyCoder, (Iterable) rawData));
          return SideInput.ready(multimapSideInputValue, data.getData().size());
        }
      default:
        {
          throw new IllegalStateException(
              "Unknown side input materialization format requested: "
                  + getViewFn(view).getMaterialization().getUrn());
        }
    }
  }

  private static <T> void validateViewMaterialization(PCollectionView<T> view) {
    String materializationUrn = getViewFn(view).getMaterialization().getUrn();
    checkState(
        SUPPORTED_MATERIALIZATIONS.contains(materializationUrn),
        "Only materialization's of type %s supported, received %s",
        SUPPORTED_MATERIALIZATIONS,
        materializationUrn);
  }

  public final long getBytesRead() {
    return bytesRead;
  }

  /**
   * Fetch the given side input, storing it in a process-level cache.
   *
   * <p>If state is KNOWN_READY, attempt to fetch the data regardless of whether a not-ready entry
   * was cached.
   */
  public <T> SideInput<T> fetchSideInput(
      PCollectionView<T> view,
      BoundedWindow sideWindow,
      String stateFamily,
      SideInputState state,
      Supplier<Closeable> scopedReadStateSupplier) {
    Callable<SideInput<T>> loadSideInputFromWindmill =
        () -> loadSideInputFromWindmill(view, sideWindow, stateFamily, scopedReadStateSupplier);
    SideInputCache.Key<T> sideInputCacheKey =
        SideInputCache.Key.create(
            getInternalTag(view), sideWindow, getViewFn(view).getTypeDescriptor());

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

  private <T, SideWindowT extends BoundedWindow> GlobalData fetchGlobalDataFromWindmill(
      PCollectionView<T> view,
      SideWindowT sideWindow,
      String stateFamily,
      Supplier<Closeable> scopedReadStateSupplier)
      throws IOException {
    @SuppressWarnings({
      "deprecation", // Internal windowStrategy is required to fetch side input data from Windmill.
      "unchecked" // Internal windowing strategy matches WindowingStrategy<?, SideWindowT>.
    })
    WindowingStrategy<?, SideWindowT> sideWindowStrategy =
        (WindowingStrategy<?, SideWindowT>) view.getWindowingStrategyInternal();

    Coder<SideWindowT> windowCoder = sideWindowStrategy.getWindowFn().windowCoder();

    ByteStringOutputStream windowStream = new ByteStringOutputStream();
    windowCoder.encode(sideWindow, windowStream);

    GlobalDataRequest request =
        GlobalDataRequest.newBuilder()
            .setDataId(
                Windmill.GlobalDataId.newBuilder()
                    .setTag(getInternalTag(view).getId())
                    .setVersion(windowStream.toByteString())
                    .build())
            .setStateFamily(stateFamily)
            .setExistenceWatermarkDeadline(
                WindmillTimeUtils.harnessToWindmillTimestamp(
                    sideWindowStrategy.getTrigger().getWatermarkThatGuaranteesFiring(sideWindow)))
            .build();

    try (Closeable ignored = scopedReadStateSupplier.get()) {
      return fetchGlobalDataFn.apply(request);
    }
  }

  private <T> SideInput<T> loadSideInputFromWindmill(
      PCollectionView<T> view,
      BoundedWindow sideWindow,
      String stateFamily,
      Supplier<Closeable> scopedReadStateSupplier)
      throws IOException {
    validateViewMaterialization(view);
    GlobalData data =
        fetchGlobalDataFromWindmill(view, sideWindow, stateFamily, scopedReadStateSupplier);
    bytesRead += data.getSerializedSize();
    return data.getIsReady() ? createSideInputCacheEntry(view, data) : SideInput.notReady();
  }
}
