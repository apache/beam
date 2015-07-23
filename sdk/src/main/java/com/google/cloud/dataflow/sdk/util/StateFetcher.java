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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.runners.worker.MetricTrackingWindmillServerStub;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Class responsible for fetching state from the windmill server.
 */
public class StateFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(StateFetcher.class);

  private Cache<SideInputId, SideInputCacheEntry> sideInputCache;

  private MetricTrackingWindmillServerStub server;

  public StateFetcher(MetricTrackingWindmillServerStub server) {
    this(server, CacheBuilder
        .newBuilder()
        .maximumWeight(100000000 /* 100 MB */)
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .weigher(new Weigher<SideInputId, SideInputCacheEntry>() {
              @Override
              public int weigh(SideInputId id, SideInputCacheEntry entry) {
                return entry.encodedSize;
              }
            })
        .build());
  }

  public StateFetcher(MetricTrackingWindmillServerStub server,
      Cache<SideInputId, SideInputCacheEntry> sideInputCache) {
    this.server = server;
    this.sideInputCache = sideInputCache;
  }

  /**
   * Indicates the caller's knowledge of whether a particular side input has been computed.
   */
  public enum SideInputState {
    CACHED_IN_WORKITEM, KNOWN_READY, UNKNOWN;
  }

  /**
   * Fetch the given side input, storing it in a process-level cache.
   *
   * <p> If state is KNOWN_READY, attempt to fetch the data regardless of whether a
   * not-ready entry was cached.
   */
  public <T, SideWindowT extends BoundedWindow> T fetchSideInput(final PCollectionView<T> view,
      final SideWindowT sideWindow, final String stateFamily, SideInputState state) {
    final SideInputId id = new SideInputId(view.getTagInternal(), sideWindow);

    Callable<SideInputCacheEntry> fetchCallable = new Callable<SideInputCacheEntry>() {
      @Override
      public SideInputCacheEntry call() throws Exception {
        @SuppressWarnings("unchecked")
        WindowingStrategy<?, SideWindowT> sideWindowStrategy =
            (WindowingStrategy<?, SideWindowT>) view.getWindowingStrategyInternal();

        Coder<SideWindowT> windowCoder = sideWindowStrategy.getWindowFn().windowCoder();

        ByteString.Output windowStream = ByteString.newOutput();
        windowCoder.encode(sideWindow, windowStream, Coder.Context.OUTER);

        @SuppressWarnings("unchecked")
        Windmill.GlobalDataRequest request =
            Windmill.GlobalDataRequest.newBuilder()
                .setDataId(Windmill.GlobalDataId.newBuilder()
                    .setTag(view.getTagInternal().getId())
                    .setVersion(windowStream.toByteString())
                    .build())
                .setStateFamily(stateFamily)
                .setExistenceWatermarkDeadline(
                     TimeUnit.MILLISECONDS.toMicros(sideWindowStrategy
                         .getTrigger().getSpec()
                         .getWatermarkThatGuaranteesFiring(sideWindow)
                         .getMillis()))
                .build();

        Windmill.GetDataResponse response = server.getSideInputData(
            Windmill.GetDataRequest.newBuilder()
            .addGlobalDataFetchRequests(request)
            .addGlobalDataToFetch(request.getDataId())
            .build());

        Windmill.GlobalData data = response.getGlobalData(0);

        Iterable<WindowedValue<?>> rawData;
        if (data.getIsReady()) {
          if (data.getData().size() > 0) {
            rawData = view.getCoderInternal().decode(
                data.getData().newInput(), Coder.Context.OUTER);
          } else {
            rawData = Collections.emptyList();
          }

          return new SideInputCacheEntry(
              view.fromIterableInternal(rawData), data.getData().size());
        } else {
          return SideInputCacheEntry.notReady();
        }
      }
    };

    try {
      if (state == SideInputState.KNOWN_READY) {
        SideInputCacheEntry entry = sideInputCache.getIfPresent(id);
        if (entry == null) {
          return (T) sideInputCache.get(id, fetchCallable).value;
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

          return (T) sideInputCache.get(id, fetchCallable).value;
        } else {
          return (T) entry.value;
        }
      } else {
        return (T) sideInputCache.get(id, fetchCallable).value;
      }
    } catch (Exception e) {
      LOG.error("Fetch failed: ", e);
      throw new RuntimeException("Exception while fetching side input: ", e);
    }
  }

  /**
   * Struct representing a side input for a particular window.
   */
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
   * Entry in the side input cache that stores the value (null if not ready), and
   * the encoded size of the value.
   */
  static class SideInputCacheEntry {
    public final Object value;
    public final int encodedSize;

    public SideInputCacheEntry(Object value, int encodedSize) {
      this.value = value;
      this.encodedSize = encodedSize;
    }

    public static SideInputCacheEntry notReady() {
      return new SideInputCacheEntry(null, 0);
    }

    public boolean isReady() {
      return value != null;
    }
  }
}
