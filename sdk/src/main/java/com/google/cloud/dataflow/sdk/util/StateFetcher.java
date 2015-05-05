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
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.runners.worker.StreamingDataflowWorker;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.WindmillServerStub;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Class responsible for fetching state from the windmill server.
 */
public class StateFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(StateFetcher.class);

  private WindmillServerStub server;
  private Cache<SideInputId, SideInputCacheEntry> sideInputCache;

  public StateFetcher(WindmillServerStub server) {
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

  public StateFetcher(
      WindmillServerStub server, Cache<SideInputId, SideInputCacheEntry> sideInputCache) {
    this.server = server;
    this.sideInputCache = sideInputCache;
  }

  public Map<CodedTupleTag<?>, Optional<?>> fetch(
      String computation, ByteString key, long workToken, String prefix,
      Iterable<? extends CodedTupleTag<?>> tags) throws CoderException, IOException {
    if (Iterables.isEmpty(tags)) {
      return Collections.emptyMap();
    }

    Windmill.KeyedGetDataRequest.Builder requestBuilder = Windmill.KeyedGetDataRequest.newBuilder()
        .setKey(key)
        .setWorkToken(workToken);


    Map<ByteString, CodedTupleTag<?>> tagMap = new HashMap<>();
    for (CodedTupleTag<?> tag : tags) {
      ByteString tagString = ByteString.copyFromUtf8(prefix + tag.getId());
      if (tagMap.put(tagString, tag) == null) {
        requestBuilder.addValuesToFetch(Windmill.TagValue.newBuilder().setTag(tagString).build());
      }
    }

    Map<CodedTupleTag<?>, Optional<?>> resultMap = new HashMap<>();
    Windmill.KeyedGetDataResponse keyResponse = getResponse(computation, key, requestBuilder);

    for (Windmill.TagValue tv : keyResponse.getValuesList()) {
      CodedTupleTag<?> tag = tagMap.get(tv.getTag());
      if (tag != null) {
        if (tv.getValue().hasData() && !tv.getValue().getData().isEmpty()) {
          Object v = tag.getCoder().decode(tv.getValue().getData().newInput(), Coder.Context.OUTER);
          resultMap.put(tag, Optional.of(v));
        } else {
          resultMap.put(tag, Optional.absent());
        }
      }
    }

    for (CodedTupleTag<?> tag : tags) {
      if (!resultMap.containsKey(tag)) {
        resultMap.put(tag, Optional.absent());
      }
    }

    return resultMap;
  }

  public Map<CodedTupleTag<?>, List<?>> fetchList(
      String computation, ByteString key, long workToken, String prefix,
      Iterable<? extends CodedTupleTag<?>> tags)
      throws IOException {
    if (Iterables.isEmpty(tags)) {
      return Collections.emptyMap();
    }

    Windmill.KeyedGetDataRequest.Builder requestBuilder = Windmill.KeyedGetDataRequest.newBuilder()
        .setKey(key)
        .setWorkToken(workToken);

    Map<ByteString, CodedTupleTag<?>> tagMap = new HashMap<>();
    for (CodedTupleTag<?> tag : tags) {
      ByteString tagString = ByteString.copyFromUtf8(prefix + tag.getId());
      if (tagMap.put(tagString, tag) == null) {
        requestBuilder.addListsToFetch(Windmill.TagList.newBuilder()
            .setTag(tagString)
            .setEndTimestamp(Long.MAX_VALUE)
            .build());
      }
    }

    Map<CodedTupleTag<?>, List<?>> resultMap = new HashMap<>();
    Windmill.KeyedGetDataResponse keyResponse = getResponse(computation, key, requestBuilder);
    for (Windmill.TagList tagList : keyResponse.getListsList()) {
      CodedTupleTag<?> tag = tagMap.get(tagList.getTag());
      resultMap.put(tag, decodeTagList(tag, tagList));
    }

    return resultMap;
  }

  private Windmill.KeyedGetDataResponse getResponse(
      String computation, ByteString key,
      Windmill.KeyedGetDataRequest.Builder requestBuilder) throws IOException {
    Windmill.GetDataRequest request = Windmill.GetDataRequest.newBuilder()
        .addRequests(
            Windmill.ComputationGetDataRequest.newBuilder()
            .setComputationId(computation)
            .addRequests(requestBuilder.build())
            .build())
        .build();
    Windmill.GetDataResponse response = server.getData(request);

    if (response.getDataCount() != 1
        || !response.getData(0).getComputationId().equals(computation)
        || response.getData(0).getDataCount() != 1) {
      throw new IOException("Invalid data response, expected single computation and key");
    }

    Windmill.KeyedGetDataResponse keyResponse = response.getData(0).getData(0);
    if (!keyResponse.getKey().equals(key)) {
      throw new IOException("Invalid data response, expected key "
          + key.toStringUtf8() + " but got " + keyResponse.getKey().toStringUtf8());
    }

    if (keyResponse.getFailed()) {
      throw new StreamingDataflowWorker.KeyTokenInvalidException(key.toStringUtf8());
    }
    return keyResponse;
  }

  private <T> List<T> decodeTagList(CodedTupleTag<T> tag, Windmill.TagList tagList)
      throws IOException {
    if (tag == null) {
      throw new IOException("Unexpected tag list for tag: " + tagList.getTag());
    }

    List<T> valueList = new ArrayList<>();
    for (Windmill.Value value : tagList.getValuesList()) {
      if (value.hasData() && !value.getData().isEmpty()) {
        valueList.add(
          // Drop the first byte of the data; it's the zero byte we prepended to avoid writing
          // empty data.
          tag.getCoder().decode(value.getData().substring(1).newInput(), Coder.Context.OUTER));
      }
    }
    return valueList;
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
  public <T> T fetchSideInput(
      final PCollectionView<T> view, final BoundedWindow window, SideInputState state) {
    final SideInputId id = new SideInputId(view.getTagInternal(), window);

    Callable<SideInputCacheEntry> fetchCallable = new Callable<SideInputCacheEntry>() {
      @Override
      public SideInputCacheEntry call() throws Exception {
        Coder<BoundedWindow> windowCoder =
            view.getWindowingStrategyInternal().getWindowFn().windowCoder();

        ByteString.Output windowStream = ByteString.newOutput();
        windowCoder.encode(window, windowStream, Coder.Context.OUTER);

        Windmill.GlobalDataRequest request =
            Windmill.GlobalDataRequest.newBuilder()
                .setDataId(Windmill.GlobalDataId.newBuilder()
                    .setTag(view.getTagInternal().getId())
                    .setVersion(windowStream.toByteString())
                    .build())
                .setExistenceWatermarkDeadline(
                     TimeUnit.MILLISECONDS.toMicros(view.getWindowingStrategyInternal()
                         .getTrigger().getSpec()
                         .getWatermarkCutoff(window)
                         .getMillis()))
                .build();

        Windmill.GetDataResponse response = server.getData(
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
