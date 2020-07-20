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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Weigher;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class responsible for fetching state from the windmill server. */
class StateFetcher {
  private static final Set<String> SUPPORTED_MATERIALIZATIONS =
      ImmutableSet.of(
          Materializations.ITERABLE_MATERIALIZATION_URN,
          Materializations.MULTIMAP_MATERIALIZATION_URN);

  private static final Logger LOG = LoggerFactory.getLogger(StateFetcher.class);

  private Cache<SideInputId, SideInputCacheEntry> sideInputCache;
  private MetricTrackingWindmillServerStub server;
  private long bytesRead = 0L;

  public StateFetcher(MetricTrackingWindmillServerStub server) {
    this(
        server,
        CacheBuilder.newBuilder()
            .maximumWeight(100000000 /* 100 MB */)
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .weigher((Weigher<SideInputId, SideInputCacheEntry>) (id, entry) -> entry.size())
            .build());
  }

  public StateFetcher(
      MetricTrackingWindmillServerStub server,
      Cache<SideInputId, SideInputCacheEntry> sideInputCache) {
    this.server = server;
    this.sideInputCache = sideInputCache;
  }

  /** Returns a view of the underlying cache that keeps track of bytes read separately. */
  public StateFetcher byteTrackingView() {
    return new StateFetcher(server, sideInputCache);
  }

  public long getBytesRead() {
    return bytesRead;
  }

  /** Indicates the caller's knowledge of whether a particular side input has been computed. */
  public enum SideInputState {
    CACHED_IN_WORKITEM,
    KNOWN_READY,
    UNKNOWN;
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
  public @Nullable <T, SideWindowT extends BoundedWindow> Optional<T> fetchSideInput(
      final PCollectionView<T> view,
      final SideWindowT sideWindow,
      final String stateFamily,
      SideInputState state,
      final Supplier<Closeable> scopedReadStateSupplier) {
    final SideInputId id = new SideInputId(view.getTagInternal(), sideWindow);

    Callable<SideInputCacheEntry> fetchCallable =
        () -> {
          @SuppressWarnings("unchecked")
          WindowingStrategy<?, SideWindowT> sideWindowStrategy =
              (WindowingStrategy<?, SideWindowT>) view.getWindowingStrategyInternal();

          Coder<SideWindowT> windowCoder = sideWindowStrategy.getWindowFn().windowCoder();

          ByteString.Output windowStream = ByteString.newOutput();
          windowCoder.encode(sideWindow, windowStream, Coder.Context.OUTER);

          @SuppressWarnings("unchecked")
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
                          sideWindowStrategy
                              .getTrigger()
                              .getWatermarkThatGuaranteesFiring(sideWindow)))
                  .build();

          Windmill.GlobalData data;
          try (Closeable scope = scopedReadStateSupplier.get()) {
            data = server.getSideInputData(request);
          }

          bytesRead += data.getSerializedSize();

          checkState(
              SUPPORTED_MATERIALIZATIONS.contains(view.getViewFn().getMaterialization().getUrn()),
              "Only materializations of type %s supported, received %s",
              SUPPORTED_MATERIALIZATIONS,
              view.getViewFn().getMaterialization().getUrn());

          Iterable<?> rawData;
          if (data.getIsReady()) {
            if (data.getData().size() > 0) {
              rawData =
                  IterableCoder.of(view.getCoderInternal())
                      .decode(data.getData().newInput(), Coder.Context.OUTER);
            } else {
              rawData = Collections.emptyList();
            }

            switch (view.getViewFn().getMaterialization().getUrn()) {
              case Materializations.ITERABLE_MATERIALIZATION_URN:
                {
                  ViewFn<IterableView, T> viewFn = (ViewFn<IterableView, T>) view.getViewFn();
                  return SideInputCacheEntry.ready(
                      viewFn.apply(() -> rawData), data.getData().size());
                }
              case Materializations.MULTIMAP_MATERIALIZATION_URN:
                {
                  ViewFn<MultimapView, T> viewFn = (ViewFn<MultimapView, T>) view.getViewFn();
                  Coder<?> keyCoder = ((KvCoder<?, ?>) view.getCoderInternal()).getKeyCoder();
                  return SideInputCacheEntry.ready(
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
          } else {
            return SideInputCacheEntry.notReady();
          }
        };

    try {
      if (state == SideInputState.KNOWN_READY) {
        SideInputCacheEntry entry = sideInputCache.getIfPresent(id);
        if (entry == null) {
          return sideInputCache.get(id, fetchCallable).getValue();
        } else if (!entry.isReady()) {
          // Invalidate the existing not-ready entry.  This must be done atomically
          // so that another thread doesn't replace the entry with a ready entry, which
          // would then be deleted here.
          synchronized (entry) {
            SideInputCacheEntry newEntry = sideInputCache.getIfPresent(id);
            if (newEntry != null && !newEntry.isReady()) {
              sideInputCache.invalidate(id);
            }
          }

          return sideInputCache.get(id, fetchCallable).getValue();
        } else {
          return entry.getValue();
        }
      } else {
        return sideInputCache.get(id, fetchCallable).getValue();
      }
    } catch (Exception e) {
      LOG.error("Fetch failed: ", e);
      throw new RuntimeException("Exception while fetching side input: ", e);
    }
  }

  /** Struct representing a side input for a particular window. */
  static class SideInputId {
    private final TupleTag<?> tag;
    private final BoundedWindow window;

    public SideInputId(TupleTag<?> tag, BoundedWindow window) {
      this.tag = tag;
      this.window = window;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof SideInputId) {
        SideInputId otherId = (SideInputId) other;
        return tag.equals(otherId.tag) && window.equals(otherId.window);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tag, window);
    }
  }

  /**
   * Entry in the side input cache that stores the value (null if not ready), and the encoded size
   * of the value.
   */
  static class SideInputCacheEntry {
    private final boolean ready;
    private final Object value;
    private final int encodedSize;

    private SideInputCacheEntry(boolean ready, Object value, int encodedSize) {
      this.ready = ready;
      this.value = value;
      this.encodedSize = encodedSize;
    }

    public static SideInputCacheEntry ready(Object value, int encodedSize) {
      return new SideInputCacheEntry(true, value, encodedSize);
    }

    public static SideInputCacheEntry notReady() {
      return new SideInputCacheEntry(false, null, 0);
    }

    public boolean isReady() {
      return ready;
    }

    /**
     * Returns {@literal null} if the side input was not ready, {@literal Optional.absent()} if the
     * side input was null, and {@literal Optional.present(...)} if the side input was non-null.
     */
    public @Nullable <T> Optional<T> getValue() {
      @SuppressWarnings("unchecked")
      T typed = (T) value;
      return ready ? Optional.fromNullable(typed) : null;
    }

    public int size() {
      return encodedSize;
    }
  }
}
