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

import com.google.api.client.util.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.KeyTokenInvalidException;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.WorkItemCancelledException;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.SortedListEntry;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.SortedListRange;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagBag;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagSortedListFetchRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagValue;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagValuePrefixRequest;
import org.apache.beam.runners.dataflow.worker.windmill.state.StateTag.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.SettableFuture;
import org.joda.time.Instant;

/**
 * Reads persistent state from {@link Windmill}. Returns {@code Future}s containing the data that
 * has been read. Will not initiate a read until {@link Future#get} is called, at which point all
 * the pending futures will be read.
 *
 * <p>CAUTION Watch out for escaping references to the reader ending up inside {@link
 * WindmillStateCache}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WindmillStateReader {
  /**
   * Ideal maximum bytes in a TagBag response. However, Windmill will always return at least one
   * value if possible irrespective of this limit.
   */
  @VisibleForTesting static final long INITIAL_MAX_BAG_BYTES = 8L << 20; // 8MB

  @VisibleForTesting static final long CONTINUATION_MAX_BAG_BYTES = 32L << 20; // 32MB

  /**
   * Ideal maximum bytes in a TagMultimapFetchResponse response. However, Windmill will always
   * return at least one value if possible irrespective of this limit.
   */
  @VisibleForTesting static final long INITIAL_MAX_MULTIMAP_BYTES = 8L << 20; // 8MB

  @VisibleForTesting static final long CONTINUATION_MAX_MULTIMAP_BYTES = 32L << 20; // 32MB

  /**
   * Ideal maximum bytes in a TagSortedList response. However, Windmill will always return at least
   * one value if possible irrespective of this limit.
   */
  @VisibleForTesting static final long MAX_ORDERED_LIST_BYTES = 8L << 20; // 8MB

  /**
   * Ideal maximum bytes in a tag-value prefix response. However, Windmill will always return at
   * least one value if possible irrespective of this limit.
   */
  @VisibleForTesting static final long MAX_TAG_VALUE_PREFIX_BYTES = 8L << 20; // 8MB

  /**
   * Ideal maximum bytes in a KeyedGetDataResponse. However, Windmill will always return at least
   * one value if possible irrespective of this limit.
   */
  @VisibleForTesting static final long MAX_KEY_BYTES = 16L << 20; // 16MB

  @VisibleForTesting static final long MAX_CONTINUATION_KEY_BYTES = 72L << 20; // 72MB
  @VisibleForTesting final ConcurrentLinkedQueue<StateTag<?>> pendingLookups;
  private final ByteString key;
  private final long shardingKey;
  private final long workToken;
  // WindmillStateReader should only perform blocking i/o in a try-with-resources block that
  // declares an AutoCloseable vended by readWrapperSupplier.
  private final Supplier<AutoCloseable> readWrapperSupplier;
  private final Function<KeyedGetDataRequest, Optional<KeyedGetDataResponse>>
      fetchStateFromWindmillFn;
  private final ConcurrentHashMap<StateTag<?>, CoderAndFuture<?>> waiting;
  private long bytesRead = 0L;
  private final Supplier<Boolean> workItemIsFailed;

  private WindmillStateReader(
      Function<KeyedGetDataRequest, Optional<KeyedGetDataResponse>> fetchStateFromWindmillFn,
      ByteString key,
      long shardingKey,
      long workToken,
      Supplier<AutoCloseable> readWrapperSupplier,
      Supplier<Boolean> workItemIsFailed) {
    this.fetchStateFromWindmillFn = fetchStateFromWindmillFn;
    this.key = key;
    this.shardingKey = shardingKey;
    this.workToken = workToken;
    this.readWrapperSupplier = readWrapperSupplier;
    this.waiting = new ConcurrentHashMap<>();
    this.pendingLookups = new ConcurrentLinkedQueue<>();
    this.workItemIsFailed = workItemIsFailed;
  }

  @VisibleForTesting
  static WindmillStateReader forTesting(
      Function<KeyedGetDataRequest, Optional<KeyedGetDataResponse>> fetchStateFromWindmillFn,
      ByteString key,
      long shardingKey,
      long workToken) {
    return new WindmillStateReader(
        fetchStateFromWindmillFn, key, shardingKey, workToken, () -> null, () -> Boolean.FALSE);
  }

  public static WindmillStateReader forWork(Work work) {
    return new WindmillStateReader(
        work::fetchKeyedState,
        work.getWorkItem().getKey(),
        work.getWorkItem().getShardingKey(),
        work.getWorkItem().getWorkToken(),
        () -> {
          work.setState(Work.State.READING);
          return () -> work.setState(Work.State.PROCESSING);
        },
        work::isFailed);
  }

  private <FutureT> Future<FutureT> stateFuture(StateTag<?> stateTag, @Nullable Coder<?> coder) {
    CoderAndFuture<FutureT> coderAndFuture = new CoderAndFuture<>(coder, SettableFuture.create());
    CoderAndFuture<?> existingCoderAndFutureWildcard =
        waiting.putIfAbsent(stateTag, coderAndFuture);
    if (existingCoderAndFutureWildcard == null) {
      // Schedule a new request. Its response is guaranteed to find the future and coder.
      pendingLookups.add(stateTag);
    } else {
      // Piggy-back on the pending or already answered request.
      @SuppressWarnings("unchecked")
      CoderAndFuture<FutureT> existingCoderAndFuture =
          (CoderAndFuture<FutureT>) existingCoderAndFutureWildcard;
      coderAndFuture = existingCoderAndFuture;
    }

    return wrappedFuture(coderAndFuture.getFuture());
  }

  private <FutureT> CoderAndFuture<FutureT> getWaiting(StateTag<?> stateTag, boolean shouldRemove) {
    CoderAndFuture<?> coderAndFutureWildcard;
    if (shouldRemove) {
      coderAndFutureWildcard = waiting.remove(stateTag);
    } else {
      coderAndFutureWildcard = waiting.get(stateTag);
    }
    if (coderAndFutureWildcard == null) {
      throw new IllegalStateException("Missing future for " + stateTag);
    }
    @SuppressWarnings("unchecked")
    CoderAndFuture<FutureT> coderAndFuture = (CoderAndFuture<FutureT>) coderAndFutureWildcard;
    return coderAndFuture;
  }

  public Future<Instant> watermarkFuture(ByteString encodedTag, String stateFamily) {
    return stateFuture(StateTag.of(StateTag.Kind.WATERMARK, encodedTag, stateFamily), null);
  }

  public <T> Future<T> valueFuture(ByteString encodedTag, String stateFamily, Coder<T> coder) {
    return stateFuture(StateTag.of(StateTag.Kind.VALUE, encodedTag, stateFamily), coder);
  }

  public <T> Future<Iterable<T>> bagFuture(
      ByteString encodedTag, String stateFamily, Coder<T> elemCoder) {
    // First request has no continuation position.
    StateTag<Long> stateTag = StateTag.of(StateTag.Kind.BAG, encodedTag, stateFamily);
    // Convert the ValuesAndContPosition<T> to Iterable<T>.
    return valuesToPagingIterableFuture(stateTag, elemCoder, this.stateFuture(stateTag, elemCoder));
  }

  public <T> Future<Iterable<TimestampedValue<T>>> orderedListFuture(
      Range<Long> range, ByteString encodedTag, String stateFamily, Coder<T> elemCoder) {
    // First request has no continuation position.
    StateTag<ByteString> stateTag =
        StateTag.<ByteString>of(StateTag.Kind.ORDERED_LIST, encodedTag, stateFamily)
            .toBuilder()
            .setSortedListRange(Preconditions.checkNotNull(range))
            .build();
    return valuesToPagingIterableFuture(stateTag, elemCoder, this.stateFuture(stateTag, elemCoder));
  }

  public <T> Future<Iterable<Map.Entry<ByteString, Iterable<T>>>> multimapFetchAllFuture(
      boolean omitValues, ByteString encodedTag, String stateFamily, Coder<T> elemCoder) {
    StateTag<ByteString> stateTag =
        StateTag.<ByteString>of(Kind.MULTIMAP_ALL, encodedTag, stateFamily)
            .toBuilder()
            .setOmitValues(omitValues)
            .build();
    return valuesToPagingIterableFuture(stateTag, elemCoder, this.stateFuture(stateTag, elemCoder));
  }

  public <T> Future<Iterable<T>> multimapFetchSingleEntryFuture(
      ByteString encodedKey, ByteString encodedTag, String stateFamily, Coder<T> elemCoder) {
    StateTag<ByteString> stateTag =
        StateTag.<ByteString>of(Kind.MULTIMAP_SINGLE_ENTRY, encodedTag, stateFamily)
            .toBuilder()
            .setMultimapKey(encodedKey)
            .build();
    return valuesToPagingIterableFuture(stateTag, elemCoder, this.stateFuture(stateTag, elemCoder));
  }

  public <V> Future<Iterable<Map.Entry<ByteString, V>>> valuePrefixFuture(
      ByteString prefix, String stateFamily, Coder<V> valueCoder) {
    // First request has no continuation position.
    StateTag<ByteString> stateTag =
        StateTag.<ByteString>of(Kind.VALUE_PREFIX, prefix, stateFamily).toBuilder().build();
    return valuesToPagingIterableFuture(
        stateTag, valueCoder, this.stateFuture(stateTag, valueCoder));
  }

  /**
   * Internal request to fetch the next 'page' of values. Return null if no continuation position is
   * in {@code contStateTag}, which signals there are no more pages.
   */
  @Nullable
  <ContinuationT, ResultT> Future<ValuesAndContPosition<ResultT, ContinuationT>> continuationFuture(
      StateTag<ContinuationT> contStateTag, Coder<?> coder) {
    if (contStateTag.getRequestPosition() == null) {
      // We're done.
      return null;
    }
    return stateFuture(contStateTag, coder);
  }

  private <T> Future<T> wrappedFuture(final Future<T> future) {
    if (future.isDone()) {
      // If the underlying lookup is already complete, we don't need to create the wrapper.
      return future;
    } else {
      // Otherwise, wrap the true future so we know when to trigger a GetData.
      return new WrappedFuture<>(this, future);
    }
  }

  /**
   * Return future which transforms a {@code ValuesAndContPosition<T>} result into the initial
   * Iterable<T> result expected from the external caller.
   */
  private <ResultT, ContinuationT> Future<Iterable<ResultT>> valuesToPagingIterableFuture(
      final StateTag<ContinuationT> stateTag,
      final Coder<?> coder,
      final Future<ValuesAndContPosition<ResultT, ContinuationT>> future) {
    Function<ValuesAndContPosition<ResultT, ContinuationT>, Iterable<ResultT>> toIterable =
        new ToIterableFunction<>(this, stateTag, coder);
    return Futures.lazyTransform(future, toIterable);
  }

  private void delayUnbatchableMultimapFetches(
      List<StateTag<?>> multimapTags, HashSet<StateTag<?>> toFetch) {
    // Each KeyedGetDataRequest can have at most 1 TagMultimapFetchRequest per <tag, state_family>
    // pair, thus we need to delay unbatchable multimap requests of the same stateFamily and tag
    // into later batches. There's no priority between get()/entries()/keys(), they will be fetched
    // based on the order they occur in pendingLookups, so that all requests can eventually be
    // fetched and none starves.

    Map<String, Map<ByteString, List<StateTag<?>>>> groupedTags =
        multimapTags.stream()
            .collect(
                Collectors.groupingBy(
                    StateTag::getStateFamily, Collectors.groupingBy(StateTag::getTag)));

    for (Map<ByteString, List<StateTag<?>>> familyTags : groupedTags.values()) {
      for (List<StateTag<?>> tags : familyTags.values()) {
        StateTag<?> first = tags.remove(0);
        toFetch.add(first);
        if (tags.isEmpty()) continue;

        if (first.getKind() == Kind.MULTIMAP_ALL) {
          // first is keys()/entries(), no more TagMultimapFetchRequests allowed in current batch.
          pendingLookups.addAll(tags);
          continue;
        }
        // first is get(key), no keys()/entries() allowed in current batch; each different key can
        // have at most one fetch request in this batch.
        Set<ByteString> addedKeys = Sets.newHashSet();
        addedKeys.add(first.getMultimapKey());
        for (StateTag<?> other : tags) {
          if (other.getKind() == Kind.MULTIMAP_ALL || addedKeys.contains(other.getMultimapKey())) {
            pendingLookups.add(other);
          } else {
            toFetch.add(other);
            addedKeys.add(other.getMultimapKey());
          }
        }
      }
    }
  }

  private void delayUnbatchableOrderedListFetches(
      List<StateTag<?>> orderedListTags, HashSet<StateTag<?>> toFetch) {
    // Each KeyedGetDataRequest can have at most 1 TagOrderedListRequest per <tag, state_family>
    // pair, thus we need to delay unbatchable ordered list requests of the same stateFamily and tag
    // into later batches.

    Map<String, Map<ByteString, List<StateTag<?>>>> groupedTags =
        orderedListTags.stream()
            .collect(
                Collectors.groupingBy(
                    StateTag::getStateFamily, Collectors.groupingBy(StateTag::getTag)));

    for (Map<ByteString, List<StateTag<?>>> familyTags : groupedTags.values()) {
      for (List<StateTag<?>> tags : familyTags.values()) {
        StateTag<?> first = tags.remove(0);
        toFetch.add(first);
        // Add the rest of the reads for the state family and tags back to pending.
        pendingLookups.addAll(tags);
      }
    }
  }

  private HashSet<StateTag<?>> buildFetchSet() {
    HashSet<StateTag<?>> toFetch = Sets.newHashSet();
    List<StateTag<?>> multimapTags = Lists.newArrayList();
    List<StateTag<?>> orderedListTags = Lists.newArrayList();
    while (!pendingLookups.isEmpty()) {
      StateTag<?> stateTag = pendingLookups.poll();
      if (stateTag == null) {
        break;
      }
      if (stateTag.getKind() == Kind.MULTIMAP_ALL
          || stateTag.getKind() == Kind.MULTIMAP_SINGLE_ENTRY) {
        multimapTags.add(stateTag);
        continue;
      }
      if (stateTag.getKind() == Kind.ORDERED_LIST) {
        orderedListTags.add(stateTag);
        continue;
      }

      if (!toFetch.add(stateTag)) {
        throw new IllegalStateException("Duplicate tags being fetched.");
      }
    }
    if (!multimapTags.isEmpty()) {
      delayUnbatchableMultimapFetches(multimapTags, toFetch);
    }
    if (!orderedListTags.isEmpty()) {
      delayUnbatchableOrderedListFetches(orderedListTags, toFetch);
    }
    return toFetch;
  }

  public void performReads() {
    while (true) {
      HashSet<StateTag<?>> toFetch = buildFetchSet();
      if (toFetch.isEmpty()) {
        return;
      }
      try {
        KeyedGetDataResponse response = tryGetDataFromWindmill(toFetch);
        // Removes tags from toFetch as they are processed.
        consumeResponse(response, toFetch);
        if (!toFetch.isEmpty()) {
          throw new IllegalStateException(
              "Didn't receive responses for all pending fetches. Missing: " + toFetch);
        }
      } catch (Exception e) {
        // Set up all the remaining futures for this key to throw an exception. This ensures that if
        // the exception is caught that all futures have been completed and do not block.
        for (StateTag<?> stateTag : toFetch) {
          waiting.get(stateTag).future.setException(e);
        }
        // Also setup futures that may have been added back if they were not batched.
        while (true) {
          @Nullable StateTag<?> stateTag = pendingLookups.poll();
          if (stateTag == null) break;
          waiting.get(stateTag).future.setException(e);
        }
        throw new RuntimeException(e);
      }
    }
  }

  private KeyedGetDataResponse tryGetDataFromWindmill(HashSet<StateTag<?>> stateTags)
      throws Exception {
    if (workItemIsFailed.get()) {
      throw new WorkItemCancelledException(shardingKey);
    }
    KeyedGetDataRequest keyedGetDataRequest = createRequest(stateTags);
    try (AutoCloseable ignored = readWrapperSupplier.get()) {
      return fetchStateFromWindmillFn
          .apply(keyedGetDataRequest)
          .orElseThrow(
              () ->
                  new RuntimeException(
                      "Windmill unexpectedly returned null for request " + keyedGetDataRequest));
    }
  }

  public long getBytesRead() {
    return bytesRead;
  }

  private KeyedGetDataRequest createRequest(Iterable<StateTag<?>> toFetch) {
    KeyedGetDataRequest.Builder keyedDataBuilder =
        KeyedGetDataRequest.newBuilder()
            .setKey(key)
            .setShardingKey(shardingKey)
            .setWorkToken(workToken);

    boolean continuation = false;
    List<StateTag<?>> orderedListsToFetch = Lists.newArrayList();
    List<StateTag<?>> multimapSingleEntryToFetch = Lists.newArrayList();
    for (StateTag<?> stateTag : toFetch) {
      switch (stateTag.getKind()) {
        case BAG:
          TagBag.Builder bag =
              keyedDataBuilder
                  .addBagsToFetchBuilder()
                  .setTag(stateTag.getTag())
                  .setStateFamily(stateTag.getStateFamily());
          if (stateTag.getRequestPosition() == null) {
            bag.setFetchMaxBytes(INITIAL_MAX_BAG_BYTES);
          } else {
            // We're asking for the next page.
            bag.setFetchMaxBytes(CONTINUATION_MAX_BAG_BYTES);
            bag.setRequestPosition((Long) stateTag.getRequestPosition());
            continuation = true;
          }
          break;

        case ORDERED_LIST:
          orderedListsToFetch.add(stateTag);
          break;

        case WATERMARK:
          keyedDataBuilder
              .addWatermarkHoldsToFetchBuilder()
              .setTag(stateTag.getTag())
              .setStateFamily(stateTag.getStateFamily());
          break;

        case VALUE:
          keyedDataBuilder
              .addValuesToFetchBuilder()
              .setTag(stateTag.getTag())
              .setStateFamily(stateTag.getStateFamily());
          break;

        case VALUE_PREFIX:
          TagValuePrefixRequest.Builder prefixFetchBuilder =
              keyedDataBuilder
                  .addTagValuePrefixesToFetchBuilder()
                  .setTagPrefix(stateTag.getTag())
                  .setStateFamily(stateTag.getStateFamily())
                  .setFetchMaxBytes(MAX_TAG_VALUE_PREFIX_BYTES);
          if (stateTag.getRequestPosition() != null) {
            prefixFetchBuilder.setRequestPosition((ByteString) stateTag.getRequestPosition());
          }
          break;

        case MULTIMAP_SINGLE_ENTRY:
          multimapSingleEntryToFetch.add(stateTag);
          break;

        case MULTIMAP_ALL:
          Windmill.TagMultimapFetchRequest.Builder multimapFetchBuilder =
              keyedDataBuilder
                  .addMultimapsToFetchBuilder()
                  .setTag(stateTag.getTag())
                  .setStateFamily(stateTag.getStateFamily())
                  .setFetchEntryNamesOnly(stateTag.getOmitValues());
          if (stateTag.getRequestPosition() == null) {
            multimapFetchBuilder.setFetchMaxBytes(INITIAL_MAX_MULTIMAP_BYTES);
          } else {
            multimapFetchBuilder.setFetchMaxBytes(CONTINUATION_MAX_MULTIMAP_BYTES);
            multimapFetchBuilder.setRequestPosition((ByteString) stateTag.getRequestPosition());
            continuation = true;
          }
          break;

        default:
          throw new RuntimeException("Unknown kind of tag requested: " + stateTag.getKind());
      }
    }
    if (!multimapSingleEntryToFetch.isEmpty()) {
      Map<String, Map<ByteString, List<StateTag<?>>>> multimapTags =
          multimapSingleEntryToFetch.stream()
              .collect(
                  Collectors.groupingBy(
                      StateTag::getStateFamily, Collectors.groupingBy(StateTag::getTag)));
      for (Map.Entry<String, Map<ByteString, List<StateTag<?>>>> entry : multimapTags.entrySet()) {
        String stateFamily = entry.getKey();
        Map<ByteString, List<StateTag<?>>> familyTags = multimapTags.get(stateFamily);
        for (Map.Entry<ByteString, List<StateTag<?>>> tagEntry : familyTags.entrySet()) {
          ByteString tag = tagEntry.getKey();
          List<StateTag<?>> stateTags = tagEntry.getValue();
          Windmill.TagMultimapFetchRequest.Builder multimapFetchBuilder =
              keyedDataBuilder
                  .addMultimapsToFetchBuilder()
                  .setTag(tag)
                  .setStateFamily(stateFamily)
                  .setFetchEntryNamesOnly(false);
          for (StateTag<?> stateTag : stateTags) {
            Windmill.TagMultimapEntry.Builder entryBuilder =
                multimapFetchBuilder
                    .addEntriesToFetchBuilder()
                    .setEntryName(stateTag.getMultimapKey());
            if (stateTag.getRequestPosition() == null) {
              entryBuilder.setFetchMaxBytes(INITIAL_MAX_BAG_BYTES);
            } else {
              entryBuilder.setFetchMaxBytes(CONTINUATION_MAX_BAG_BYTES);
              entryBuilder.setRequestPosition((Long) stateTag.getRequestPosition());
              continuation = true;
            }
          }
        }
      }
    }

    orderedListsToFetch.sort(
        Comparator.<StateTag<?>>comparingLong(s -> s.getSortedListRange().lowerEndpoint())
            .thenComparingLong(s -> s.getSortedListRange().upperEndpoint()));
    for (StateTag<?> stateTag : orderedListsToFetch) {
      Range<Long> range = Preconditions.checkNotNull(stateTag.getSortedListRange());
      TagSortedListFetchRequest.Builder sorted_list =
          keyedDataBuilder
              .addSortedListsToFetchBuilder()
              .setTag(stateTag.getTag())
              .setStateFamily(stateTag.getStateFamily())
              .setFetchMaxBytes(MAX_ORDERED_LIST_BYTES);
      sorted_list.addFetchRanges(
          SortedListRange.newBuilder()
              .setStart(range.lowerEndpoint())
              .setLimit(range.upperEndpoint())
              .build());
      if (stateTag.getRequestPosition() != null) {
        // We're asking for the next page.
        sorted_list.setRequestPosition((ByteString) stateTag.getRequestPosition());
      }
    }
    if (continuation) {
      keyedDataBuilder.setMaxBytes(MAX_CONTINUATION_KEY_BYTES);
    } else {
      keyedDataBuilder.setMaxBytes(MAX_KEY_BYTES);
    }

    return keyedDataBuilder.build();
  }

  private void consumeResponse(KeyedGetDataResponse response, Set<StateTag<?>> toFetch) {
    bytesRead += response.getSerializedSize();
    if (response.getFailed()) {
      throw new KeyTokenInvalidException(key.toStringUtf8());
    }

    if (!key.equals(response.getKey())) {
      throw new RuntimeException("Expected data for key " + key + " but was " + response.getKey());
    }

    for (Windmill.TagBag bag : response.getBagsList()) {
      StateTag<Long> stateTag =
          StateTag.of(
              StateTag.Kind.BAG,
              bag.getTag(),
              bag.getStateFamily(),
              bag.hasRequestPosition() ? bag.getRequestPosition() : null);
      if (!toFetch.remove(stateTag)) {
        throw new IllegalStateException(
            "Received response for unrequested tag " + stateTag + ". Pending tags: " + toFetch);
      }
      consumeBag(bag, stateTag);
    }

    for (Windmill.WatermarkHold hold : response.getWatermarkHoldsList()) {
      StateTag<Long> stateTag =
          StateTag.of(StateTag.Kind.WATERMARK, hold.getTag(), hold.getStateFamily());
      if (!toFetch.remove(stateTag)) {
        throw new IllegalStateException(
            "Received response for unrequested tag " + stateTag + ". Pending tags: " + toFetch);
      }
      consumeWatermark(hold, stateTag);
    }

    for (Windmill.TagValue value : response.getValuesList()) {
      StateTag<Long> stateTag =
          StateTag.of(StateTag.Kind.VALUE, value.getTag(), value.getStateFamily());
      if (!toFetch.remove(stateTag)) {
        throw new IllegalStateException(
            "Received response for unrequested tag " + stateTag + ". Pending tags: " + toFetch);
      }
      consumeTagValue(value, stateTag);
    }
    for (Windmill.TagValuePrefixResponse prefix_response : response.getTagValuePrefixesList()) {
      StateTag<ByteString> stateTag =
          StateTag.of(
              Kind.VALUE_PREFIX,
              prefix_response.getTagPrefix(),
              prefix_response.getStateFamily(),
              prefix_response.hasRequestPosition() ? prefix_response.getRequestPosition() : null);
      if (!toFetch.remove(stateTag)) {
        throw new IllegalStateException(
            "Received response for unrequested tag " + stateTag + ". Pending tags: " + toFetch);
      }
      consumeTagPrefixResponse(prefix_response, stateTag);
    }
    for (Windmill.TagSortedListFetchResponse sorted_list : response.getTagSortedListsList()) {
      SortedListRange sortedListRange = Iterables.getOnlyElement(sorted_list.getFetchRangesList());
      Range<Long> range = Range.closedOpen(sortedListRange.getStart(), sortedListRange.getLimit());
      StateTag<ByteString> stateTag =
          StateTag.of(
                  StateTag.Kind.ORDERED_LIST,
                  sorted_list.getTag(),
                  sorted_list.getStateFamily(),
                  sorted_list.hasRequestPosition() ? sorted_list.getRequestPosition() : null)
              .toBuilder()
              .setSortedListRange(range)
              .build();
      if (!toFetch.remove(stateTag)) {
        throw new IllegalStateException(
            "Received response for unrequested tag " + stateTag + ". Pending tags: " + toFetch);
      }

      consumeSortedList(sorted_list, stateTag);
    }

    for (Windmill.TagMultimapFetchResponse tagMultimap : response.getTagMultimapsList()) {
      // First check if it's keys()/entries()
      StateTag.Builder<ByteString> builder =
          StateTag.of(
                  Kind.MULTIMAP_ALL,
                  tagMultimap.getTag(),
                  tagMultimap.getStateFamily(),
                  tagMultimap.hasRequestPosition() ? tagMultimap.getRequestPosition() : null)
              .toBuilder();
      StateTag<ByteString> tag = builder.setOmitValues(true).build();
      if (toFetch.contains(tag)) {
        // this is keys()
        toFetch.remove(tag);
        consumeMultimapAll(tagMultimap, tag);
        continue;
      }
      tag = builder.setOmitValues(false).build();
      if (toFetch.contains(tag)) {
        // this is entries()
        toFetch.remove(tag);
        consumeMultimapAll(tagMultimap, tag);
        continue;
      }
      // this is get()
      StateTag.Builder<Long> entryTagBuilder =
          StateTag.<Long>of(
                  Kind.MULTIMAP_SINGLE_ENTRY, tagMultimap.getTag(), tagMultimap.getStateFamily())
              .toBuilder();
      StateTag<Long> entryTag = null;
      for (Windmill.TagMultimapEntry entry : tagMultimap.getEntriesList()) {
        entryTag =
            entryTagBuilder
                .setMultimapKey(entry.getEntryName())
                .setRequestPosition(entry.hasRequestPosition() ? entry.getRequestPosition() : null)
                .build();
        if (!toFetch.remove(entryTag)) {
          throw new IllegalStateException(
              "Received response for unrequested tag " + entryTag + ". Pending tags: " + toFetch);
        }
        consumeMultimapSingleEntry(entry, entryTag);
      }
    }
  }

  /** The deserialized values in {@code bag} as a read-only array list. */
  private <T> List<T> bagPageValues(TagBag bag, Coder<T> elemCoder) {
    if (bag.getValuesCount() == 0) {
      return new WeightedList<T>(Collections.emptyList());
    }

    WeightedList<T> valueList = new WeightedList<>(new ArrayList<T>(bag.getValuesCount()));
    for (ByteString value : bag.getValuesList()) {
      try {
        valueList.addWeighted(
            elemCoder.decode(value.newInput(), Coder.Context.OUTER), value.size());
      } catch (IOException e) {
        throw new IllegalStateException("Unable to decode tag list using " + elemCoder, e);
      }
    }
    return valueList;
  }

  private <T> List<TimestampedValue<T>> sortedListPageValues(
      Windmill.TagSortedListFetchResponse sortedListFetchResponse, Coder<T> elemCoder) {
    if (sortedListFetchResponse.getEntriesCount() == 0) {
      return new WeightedList<>(Collections.emptyList());
    }

    WeightedList<TimestampedValue<T>> entryList =
        new WeightedList<>(new ArrayList<>(sortedListFetchResponse.getEntriesCount()));
    for (SortedListEntry entry : sortedListFetchResponse.getEntriesList()) {
      try {
        T value = elemCoder.decode(entry.getValue().newInput(), Coder.Context.OUTER);
        entryList.addWeighted(
            TimestampedValue.of(
                value, WindmillTimeUtils.windmillToHarnessTimestamp(entry.getSortKey())),
            entry.getValue().size() + 8);
      } catch (IOException e) {
        throw new IllegalStateException("Unable to decode tag sorted list using " + elemCoder, e);
      }
    }
    return entryList;
  }

  private <V> List<Map.Entry<ByteString, V>> tagPrefixPageTagValues(
      Windmill.TagValuePrefixResponse tagValuePrefixResponse, Coder<V> valueCoder) {
    if (tagValuePrefixResponse.getTagValuesCount() == 0) {
      return new WeightedList<>(Collections.emptyList());
    }

    WeightedList<Map.Entry<ByteString, V>> entryList =
        new WeightedList<Map.Entry<ByteString, V>>(
            new ArrayList<>(tagValuePrefixResponse.getTagValuesCount()));
    for (TagValue entry : tagValuePrefixResponse.getTagValuesList()) {
      try {
        V value = valueCoder.decode(entry.getValue().getData().newInput(), Context.OUTER);
        entryList.addWeighted(
            new AbstractMap.SimpleEntry<>(entry.getTag(), value),
            entry.getTag().size() + entry.getValue().getData().size());
      } catch (IOException e) {
        throw new IllegalStateException("Unable to decode tag value " + e);
      }
    }
    return entryList;
  }

  private <V> WeightedList<V> multimapEntryPageValues(
      Windmill.TagMultimapEntry entry, Coder<V> valueCoder) {
    if (entry.getValuesCount() == 0) {
      return new WeightedList<>(Collections.emptyList());
    }
    WeightedList<V> valuesList = new WeightedList<>(new ArrayList<>(entry.getValuesCount()));
    for (ByteString value : entry.getValuesList()) {
      try {
        V decoded = valueCoder.decode(value.newInput(), Context.OUTER);
        valuesList.addWeighted(decoded, value.size());
      } catch (IOException e) {
        throw new IllegalStateException("Unable to decode multimap value " + e);
      }
    }
    return valuesList;
  }

  private <V> List<Map.Entry<ByteString, Iterable<V>>> multimapPageValues(
      Windmill.TagMultimapFetchResponse response, Coder<V> valueCoder) {
    if (response.getEntriesCount() == 0) {
      return new WeightedList<>(Collections.emptyList());
    }
    WeightedList<Map.Entry<ByteString, Iterable<V>>> entriesList =
        new WeightedList<>(new ArrayList<>(response.getEntriesCount()));
    for (Windmill.TagMultimapEntry entry : response.getEntriesList()) {
      WeightedList<V> values = multimapEntryPageValues(entry, valueCoder);
      entriesList.addWeighted(
          new AbstractMap.SimpleEntry<>(entry.getEntryName(), values),
          entry.getEntryName().size() + values.getWeight());
    }
    return entriesList;
  }

  private <T> void consumeBag(TagBag bag, StateTag<Long> stateTag) {
    boolean shouldRemove;
    // This is the response for the first page.
    // Leave the future in the cache so subsequent requests for the first page
    // can return immediately.
    // This is a response for a subsequent page.
    // Don't cache the future since we may need to make multiple requests with different
    // continuation positions.
    shouldRemove = stateTag.getRequestPosition() != null;
    CoderAndFuture<ValuesAndContPosition<T, Long>> coderAndFuture =
        getWaiting(stateTag, shouldRemove);
    SettableFuture<ValuesAndContPosition<T, Long>> future =
        coderAndFuture.getNonDoneFuture(stateTag);
    try {
      Coder<T> coder = coderAndFuture.getAndClearCoder();
      List<T> values = this.bagPageValues(bag, coder);
      future.set(
          new ValuesAndContPosition<>(
              values, bag.hasContinuationPosition() ? bag.getContinuationPosition() : null));
    } catch (Exception e) {
      future.setException(new RuntimeException("Error parsing bag response", e));
    }
  }

  private void consumeWatermark(Windmill.WatermarkHold watermarkHold, StateTag<Long> stateTag) {
    CoderAndFuture<Instant> coderAndFuture = getWaiting(stateTag, false);
    SettableFuture<Instant> future = coderAndFuture.getNonDoneFuture(stateTag);
    // No coders for watermarks
    coderAndFuture.checkNoCoder();

    Instant hold = null;
    for (long timestamp : watermarkHold.getTimestampsList()) {
      Instant instant = new Instant(TimeUnit.MICROSECONDS.toMillis(timestamp));
      // TIMESTAMP_MAX_VALUE represents infinity, and windmill will return it if no hold is set, so
      // don't treat it as a hold here.
      if (instant.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)
          && (hold == null || instant.isBefore(hold))) {
        hold = instant;
      }
    }

    future.set(hold);
  }

  private <T> void consumeTagValue(TagValue tagValue, StateTag<Long> stateTag) {
    CoderAndFuture<T> coderAndFuture = getWaiting(stateTag, false);
    SettableFuture<T> future = coderAndFuture.getNonDoneFuture(stateTag);
    Coder<T> coder = coderAndFuture.getAndClearCoder();

    if (tagValue.hasValue()
        && tagValue.getValue().hasData()
        && !tagValue.getValue().getData().isEmpty()) {
      InputStream inputStream = tagValue.getValue().getData().newInput();
      try {
        T value = coder.decode(inputStream, Coder.Context.OUTER);
        future.set(value);
      } catch (IOException e) {
        future.setException(new IllegalStateException("Unable to decode value using " + coder, e));
      }
    } else {
      future.set(null);
    }
  }

  private <V> void consumeTagPrefixResponse(
      Windmill.TagValuePrefixResponse tagValuePrefixResponse, StateTag<ByteString> stateTag) {
    boolean shouldRemove;
    // This is the response for the first page.
    // Leave the future in the cache so subsequent
    // requests for the first page
    // can return immediately.
    // This is a response for a subsequent page.
    // Don't cache the future since we may need to make multiple requests with different
    // continuation positions.
    shouldRemove = stateTag.getRequestPosition() != null;

    CoderAndFuture<ValuesAndContPosition<Map.Entry<ByteString, V>, ByteString>> coderAndFuture =
        getWaiting(stateTag, shouldRemove);
    SettableFuture<ValuesAndContPosition<Map.Entry<ByteString, V>, ByteString>> future =
        coderAndFuture.getNonDoneFuture(stateTag);
    Coder<V> valueCoder = coderAndFuture.getAndClearCoder();
    try {
      List<Map.Entry<ByteString, V>> values =
          this.tagPrefixPageTagValues(tagValuePrefixResponse, valueCoder);
      future.set(
          new ValuesAndContPosition<>(
              values,
              tagValuePrefixResponse.hasContinuationPosition()
                  ? tagValuePrefixResponse.getContinuationPosition()
                  : null));
    } catch (Exception e) {
      future.setException(new RuntimeException("Error parsing tag value prefix", e));
    }
  }

  private <T> void consumeSortedList(
      Windmill.TagSortedListFetchResponse sortedListFetchResponse, StateTag<ByteString> stateTag) {
    boolean shouldRemove;
    // This is the response for the first page.// Leave the future in the cache so subsequent
    // requests for the first page
    // can return immediately.
    // This is a response for a subsequent page.
    // Don't cache the future since we may need to make multiple requests with different
    // continuation positions.
    shouldRemove = stateTag.getRequestPosition() != null;

    CoderAndFuture<ValuesAndContPosition<TimestampedValue<T>, ByteString>> coderAndFuture =
        getWaiting(stateTag, shouldRemove);
    SettableFuture<ValuesAndContPosition<TimestampedValue<T>, ByteString>> future =
        coderAndFuture.getNonDoneFuture(stateTag);
    Coder<T> coder = coderAndFuture.getAndClearCoder();
    try {
      List<TimestampedValue<T>> values = this.sortedListPageValues(sortedListFetchResponse, coder);
      future.set(
          new ValuesAndContPosition<>(
              values,
              sortedListFetchResponse.hasContinuationPosition()
                  ? sortedListFetchResponse.getContinuationPosition()
                  : null));
    } catch (Exception e) {
      future.setException(new RuntimeException("Error parsing ordered list", e));
    }
  }

  private <V> void consumeMultimapAll(
      Windmill.TagMultimapFetchResponse response, StateTag<ByteString> stateTag) {
    // Leave the future in the cache for the first page; do not cache for subsequent pages.
    boolean shouldRemove = stateTag.getRequestPosition() != null;
    CoderAndFuture<ValuesAndContPosition<Map.Entry<ByteString, Iterable<V>>, ByteString>>
        coderAndFuture = getWaiting(stateTag, shouldRemove);
    SettableFuture<ValuesAndContPosition<Map.Entry<ByteString, Iterable<V>>, ByteString>> future =
        coderAndFuture.getNonDoneFuture(stateTag);
    Coder<V> valueCoder = coderAndFuture.getAndClearCoder();
    try {
      List<Map.Entry<ByteString, Iterable<V>>> entries =
          this.multimapPageValues(response, valueCoder);
      future.set(
          new ValuesAndContPosition<>(
              entries,
              response.hasContinuationPosition() ? response.getContinuationPosition() : null));
    } catch (Exception e) {
      future.setException(new RuntimeException("Error parsing multimap fetch all result", e));
    }
  }

  private <V> void consumeMultimapSingleEntry(
      Windmill.TagMultimapEntry entry, StateTag<Long> stateTag) {
    // Leave the future in the cache for the first page; do not cache for subsequent pages.
    boolean shouldRemove = stateTag.getRequestPosition() != null;
    CoderAndFuture<ValuesAndContPosition<V, Long>> coderAndFuture =
        getWaiting(stateTag, shouldRemove);
    SettableFuture<ValuesAndContPosition<V, Long>> future =
        coderAndFuture.getNonDoneFuture(stateTag);
    Coder<V> valueCoder = coderAndFuture.getAndClearCoder();
    try {
      List<V> values = this.multimapEntryPageValues(entry, valueCoder);
      Long continuationToken =
          entry.hasContinuationPosition() ? entry.getContinuationPosition() : null;
      future.set(new ValuesAndContPosition<>(values, continuationToken));
    } catch (Exception e) {
      future.setException(new RuntimeException("Error parsing multimap single entry result", e));
    }
  }

  private static final class CoderAndFuture<FutureT> {
    private final SettableFuture<FutureT> future;
    private Coder<?> coder = null;

    private CoderAndFuture(Coder<?> coder, SettableFuture<FutureT> future) {
      this.coder = coder;
      this.future = future;
    }

    private SettableFuture<FutureT> getFuture() {
      return future;
    }

    private SettableFuture<FutureT> getNonDoneFuture(StateTag stateTag) {
      if (future.isDone()) {
        throw new IllegalStateException("Future for " + stateTag + " is already done");
      }
      return future;
    }

    private <ElemT> Coder<ElemT> getAndClearCoder() {
      if (coder == null) {
        throw new IllegalStateException("Coder has already been cleared from cache");
      }
      Coder<ElemT> result = (Coder<ElemT>) coder;
      if (result == null) {
        throw new IllegalStateException("Coder has already been cleared from cache");
      }
      coder = null;
      return result;
    }

    private void checkNoCoder() {
      if (coder != null) {
        throw new IllegalStateException("Unexpected coder");
      }
    }
  }
}
