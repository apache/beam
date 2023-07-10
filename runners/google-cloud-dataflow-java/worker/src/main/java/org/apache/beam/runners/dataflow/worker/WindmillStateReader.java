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

import com.google.api.client.util.Lists;
import com.google.auto.value.AutoValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.WindmillStateReader.StateTag.Kind;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.SortedListEntry;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.SortedListRange;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagBag;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagSortedListFetchRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagValue;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagValuePrefixRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ForwardingList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ForwardingFuture;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.SettableFuture;
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
class WindmillStateReader {
  /**
   * Ideal maximum bytes in a TagBag response. However, Windmill will always return at least one
   * value if possible irrespective of this limit.
   */
  public static final long INITIAL_MAX_BAG_BYTES = 8L << 20; // 8MB

  public static final long CONTINUATION_MAX_BAG_BYTES = 32L << 20; // 32MB

  /**
   * Ideal maximum bytes in a TagMultimapFetchResponse response. However, Windmill will always
   * return at least one value if possible irrespective of this limit.
   */
  public static final long INITIAL_MAX_MULTIMAP_BYTES = 8L << 20; // 8MB

  public static final long CONTINUATION_MAX_MULTIMAP_BYTES = 32L << 20; // 32MB

  /**
   * Ideal maximum bytes in a TagSortedList response. However, Windmill will always return at least
   * one value if possible irrespective of this limit.
   */
  public static final long MAX_ORDERED_LIST_BYTES = 8L << 20; // 8MB

  /**
   * Ideal maximum bytes in a tag-value prefix response. However, Windmill will always return at
   * least one value if possible irrespective of this limit.
   */
  public static final long MAX_TAG_VALUE_PREFIX_BYTES = 8L << 20; // 8MB

  /**
   * Ideal maximum bytes in a KeyedGetDataResponse. However, Windmill will always return at least
   * one value if possible irrespective of this limit.
   */
  public static final long MAX_KEY_BYTES = 16L << 20; // 16MB

  public static final long MAX_CONTINUATION_KEY_BYTES = 72L << 20; // 72MB

  /**
   * When combined with a key and computationId, represents the unique address for state managed by
   * Windmill.
   */
  @AutoValue
  abstract static class StateTag<RequestPositionT> {
    enum Kind {
      VALUE,
      BAG,
      WATERMARK,
      ORDERED_LIST,
      VALUE_PREFIX,
      MULTIMAP_SINGLE_ENTRY,
      MULTIMAP_ALL
    }

    abstract Kind getKind();

    abstract ByteString getTag();

    abstract String getStateFamily();

    /**
     * For {@link Kind#BAG, Kind#ORDERED_LIST, Kind#VALUE_PREFIX, KIND#MULTIMAP_SINGLE_ENTRY,
     * KIND#MULTIMAP_ALL} kinds: A previous 'continuation_position' returned by Windmill to signal
     * the resulting state was incomplete. Sending that position will request the next page of
     * values. Null for first request.
     *
     * <p>Null for other kinds.
     */
    @Nullable
    abstract RequestPositionT getRequestPosition();

    /** For {@link Kind#ORDERED_LIST} kinds: the range to fetch or delete. */
    @Nullable
    abstract Range<Long> getSortedListRange();

    /** For {@link Kind#MULTIMAP_SINGLE_ENTRY} kinds: the key in the multimap to fetch or delete. */
    @Nullable
    abstract ByteString getMultimapKey();

    /**
     * For {@link Kind#MULTIMAP_ALL} kinds: will only return the keys of the multimap and not the
     * values if true.
     */
    @Nullable
    abstract Boolean getOmitValues();

    static <RequestPositionT> StateTag<RequestPositionT> of(
        Kind kind, ByteString tag, String stateFamily, @Nullable RequestPositionT requestPosition) {
      return new AutoValue_WindmillStateReader_StateTag.Builder<RequestPositionT>()
          .setKind(kind)
          .setTag(tag)
          .setStateFamily(stateFamily)
          .setRequestPosition(requestPosition)
          .build();
    }

    static <RequestPositionT> StateTag<RequestPositionT> of(
        Kind kind, ByteString tag, String stateFamily) {
      return of(kind, tag, stateFamily, null);
    }

    abstract Builder<RequestPositionT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<RequestPositionT> {
      abstract Builder<RequestPositionT> setKind(Kind kind);

      abstract Builder<RequestPositionT> setTag(ByteString tag);

      abstract Builder<RequestPositionT> setStateFamily(String stateFamily);

      abstract Builder<RequestPositionT> setRequestPosition(
          @Nullable RequestPositionT requestPosition);

      abstract Builder<RequestPositionT> setSortedListRange(@Nullable Range<Long> sortedListRange);

      abstract Builder<RequestPositionT> setMultimapKey(@Nullable ByteString encodedMultimapKey);

      abstract Builder<RequestPositionT> setOmitValues(Boolean omitValues);

      abstract StateTag<RequestPositionT> build();
    }
  }

  /**
   * An in-memory collection of deserialized values and an optional continuation position to pass to
   * Windmill when fetching the next page of values.
   */
  private static class ValuesAndContPosition<T, ContinuationT> {
    private final List<T> values;

    /** Position to pass to next request for next page of values. Null if done. */
    private final @Nullable ContinuationT continuationPosition;

    public ValuesAndContPosition(List<T> values, @Nullable ContinuationT continuationPosition) {
      this.values = values;
      this.continuationPosition = continuationPosition;
    }
  }

  private final String computation;
  private final ByteString key;
  private final long shardingKey;
  private final long workToken;

  // WindmillStateReader should only perform blocking i/o in a try-with-resources block that
  // declares an AutoCloseable vended by readWrapperSupplier.
  private final Supplier<AutoCloseable> readWrapperSupplier;

  private final MetricTrackingWindmillServerStub server;

  private long bytesRead = 0L;

  public WindmillStateReader(
      MetricTrackingWindmillServerStub server,
      String computation,
      ByteString key,
      long shardingKey,
      long workToken,
      Supplier<AutoCloseable> readWrapperSupplier) {
    this.server = server;
    this.computation = computation;
    this.key = key;
    this.shardingKey = shardingKey;
    this.workToken = workToken;
    this.readWrapperSupplier = readWrapperSupplier;
  }

  public WindmillStateReader(
      MetricTrackingWindmillServerStub server,
      String computation,
      ByteString key,
      long shardingKey,
      long workToken) {
    this(server, computation, key, shardingKey, workToken, () -> null);
  }

  private static final class CoderAndFuture<FutureT> {
    private Coder<?> coder = null;
    private final SettableFuture<FutureT> future;

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

  @VisibleForTesting
  ConcurrentLinkedQueue<StateTag<?>> pendingLookups = new ConcurrentLinkedQueue<>();

  private ConcurrentHashMap<StateTag<?>, CoderAndFuture<?>> waiting = new ConcurrentHashMap<>();

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
  private @Nullable <ContinuationT, ResultT>
      Future<ValuesAndContPosition<ResultT, ContinuationT>> continuationFuture(
          StateTag<ContinuationT> contStateTag, Coder<?> coder) {
    if (contStateTag.getRequestPosition() == null) {
      // We're done.
      return null;
    }
    return stateFuture(contStateTag, coder);
  }

  /**
   * A future which will trigger a GetData request to Windmill for all outstanding futures on the
   * first {@link #get}.
   */
  private static class WrappedFuture<T> extends ForwardingFuture.SimpleForwardingFuture<T> {
    /**
     * The reader we'll use to service the eventual read. Null if read has been fulfilled.
     *
     * <p>NOTE: We must clear this after the read is fulfilled to prevent space leaks.
     */
    private @Nullable WindmillStateReader reader;

    public WrappedFuture(WindmillStateReader reader, Future<T> delegate) {
      super(delegate);
      this.reader = reader;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      if (!delegate().isDone() && reader != null) {
        // Only one thread per reader, so no race here.
        reader.startBatchAndBlock();
      }
      reader = null;
      return super.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (!delegate().isDone() && reader != null) {
        // Only one thread per reader, so no race here.
        reader.startBatchAndBlock();
      }
      reader = null;
      return super.get(timeout, unit);
    }
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

  /** Function to extract an {@link Iterable} from the continuation-supporting page read future. */
  private static class ToIterableFunction<ContinuationT, ResultT>
      implements Function<ValuesAndContPosition<ResultT, ContinuationT>, Iterable<ResultT>> {
    /**
     * Reader to request continuation pages from, or {@literal null} if no continuation pages
     * required.
     */
    private @Nullable WindmillStateReader reader;

    private final StateTag<ContinuationT> stateTag;
    private final Coder<?> coder;

    public ToIterableFunction(
        WindmillStateReader reader, StateTag<ContinuationT> stateTag, Coder<?> coder) {
      this.reader = reader;
      this.stateTag = stateTag;
      this.coder = coder;
    }

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public Iterable<ResultT> apply(
        @Nonnull ValuesAndContPosition<ResultT, ContinuationT> valuesAndContPosition) {
      if (valuesAndContPosition.continuationPosition == null) {
        // Number of values is small enough Windmill sent us the entire bag in one response.
        reader = null;
        return valuesAndContPosition.values;
      } else {
        // Return an iterable which knows how to come back for more.
        StateTag.Builder<ContinuationT> continuationTBuilder =
            StateTag.of(
                    stateTag.getKind(),
                    stateTag.getTag(),
                    stateTag.getStateFamily(),
                    valuesAndContPosition.continuationPosition)
                .toBuilder();
        if (stateTag.getSortedListRange() != null) {
          continuationTBuilder.setSortedListRange(stateTag.getSortedListRange()).build();
        }
        if (stateTag.getMultimapKey() != null) {
          continuationTBuilder.setMultimapKey(stateTag.getMultimapKey()).build();
        }
        if (stateTag.getOmitValues() != null) {
          continuationTBuilder.setOmitValues(stateTag.getOmitValues()).build();
        }
        return new PagingIterable<ContinuationT, ResultT>(
            reader, valuesAndContPosition.values, continuationTBuilder.build(), coder);
      }
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

  public void startBatchAndBlock() {
    // First, drain work out of the pending lookups into a set. These will be the items we fetch.
    HashSet<StateTag<?>> toFetch = Sets.newHashSet();
    try {
      List<StateTag<?>> multimapTags = Lists.newArrayList();
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
        if (!toFetch.add(stateTag)) {
          throw new IllegalStateException("Duplicate tags being fetched.");
        }
      }
      if (!multimapTags.isEmpty()) {
        delayUnbatchableMultimapFetches(multimapTags, toFetch);
      }

      // If we failed to drain anything, some other thread pulled it off the queue. We have no work
      // to do.
      if (toFetch.isEmpty()) {
        return;
      }

      Windmill.KeyedGetDataRequest request = createRequest(toFetch);
      Windmill.KeyedGetDataResponse response;
      try (AutoCloseable readWrapper = readWrapperSupplier.get()) {
        response = server.getStateData(computation, request);
      }
      if (response == null) {
        throw new RuntimeException("Windmill unexpectedly returned null for request " + request);
      }

      // Removes tags from toFetch as they are processed.
      consumeResponse(response, toFetch);
    } catch (Exception e) {
      // Set up all the remaining futures for this key to throw an exception. This ensures that if
      // the exception is caught that all futures have been completed and do not block.
      for (StateTag<?> stateTag : toFetch) {
        waiting.get(stateTag).future.setException(e);
      }
      throw new RuntimeException(e);
    }
  }

  public long getBytesRead() {
    return bytesRead;
  }

  private Windmill.KeyedGetDataRequest createRequest(Iterable<StateTag<?>> toFetch) {
    Windmill.KeyedGetDataRequest.Builder keyedDataBuilder =
        Windmill.KeyedGetDataRequest.newBuilder()
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

  private void consumeResponse(Windmill.KeyedGetDataResponse response, Set<StateTag<?>> toFetch) {
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

    if (!toFetch.isEmpty()) {
      throw new IllegalStateException(
          "Didn't receive responses for all pending fetches. Missing: " + toFetch);
    }
  }

  @VisibleForTesting
  static class WeightedList<T> extends ForwardingList<T> implements Weighted {
    private List<T> delegate;
    long weight;

    WeightedList(List<T> delegate) {
      this.delegate = delegate;
      this.weight = 0;
    }

    @Override
    protected List<T> delegate() {
      return delegate;
    }

    @Override
    public boolean add(T elem) {
      throw new UnsupportedOperationException("Must use AddWeighted()");
    }

    @Override
    public long getWeight() {
      return weight;
    }

    public void addWeighted(T elem, long weight) {
      delegate.add(elem);
      this.weight += weight;
    }
  }

  /** The deserialized values in {@code bag} as a read-only array list. */
  private <T> List<T> bagPageValues(TagBag bag, Coder<T> elemCoder) {
    if (bag.getValuesCount() == 0) {
      return new WeightedList<T>(Collections.<T>emptyList());
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
    if (stateTag.getRequestPosition() == null) {
      // This is the response for the first page.
      // Leave the future in the cache so subsequent requests for the first page
      // can return immediately.
      shouldRemove = false;
    } else {
      // This is a response for a subsequent page.
      // Don't cache the future since we may need to make multiple requests with different
      // continuation positions.
      shouldRemove = true;
    }
    CoderAndFuture<ValuesAndContPosition<T, Long>> coderAndFuture =
        getWaiting(stateTag, shouldRemove);
    SettableFuture<ValuesAndContPosition<T, Long>> future =
        coderAndFuture.getNonDoneFuture(stateTag);
    try {
      Coder<T> coder = coderAndFuture.<T>getAndClearCoder();
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
    if (stateTag.getRequestPosition() == null) {
      // This is the response for the first page.
      // Leave the future in the cache so subsequent
      // requests for the first page
      // can return immediately.
      shouldRemove = false;
    } else {
      // This is a response for a subsequent page.
      // Don't cache the future since we may need to make multiple requests with different
      // continuation positions.
      shouldRemove = true;
    }

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
    if (stateTag.getRequestPosition() == null) {
      // This is the response for the first page.// Leave the future in the cache so subsequent
      // requests for the first page
      // can return immediately.
      shouldRemove = false;
    } else {
      // This is a response for a subsequent page.
      // Don't cache the future since we may need to make multiple requests with different
      // continuation positions.
      shouldRemove = true;
    }

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

  /**
   * An iterable over elements backed by paginated GetData requests to Windmill. The iterable may be
   * iterated over an arbitrary number of times and multiple iterators may be active simultaneously.
   *
   * <p>There are two pattern we wish to support with low -memory and -latency:
   *
   * <ol>
   *   <li>Re-iterate over the initial elements multiple times (eg Iterables.first). We'll cache the
   *       initial 'page' of values returned by Windmill from our first request for the lifetime of
   *       the iterable.
   *   <li>Iterate through all elements of a very large collection. We'll send the GetData request
   *       for the next page when the current page is begun. We'll discard intermediate pages and
   *       only retain the first. Thus the maximum memory pressure is one page plus one page per
   *       call to iterator.
   * </ol>
   */
  private static class PagingIterable<ContinuationT, ResultT> implements Iterable<ResultT> {
    /**
     * The reader we will use for scheduling continuation pages.
     *
     * <p>NOTE We've made this explicit to remind us to be careful not to cache the iterable.
     */
    private final WindmillStateReader reader;

    /** Initial values returned for the first page. Never reclaimed. */
    private final List<ResultT> firstPage;

    /** State tag with continuation position set for second page. */
    private final StateTag<ContinuationT> secondPagePos;

    /** Coder for elements. */
    private final Coder<?> coder;

    private PagingIterable(
        WindmillStateReader reader,
        List<ResultT> firstPage,
        StateTag<ContinuationT> secondPagePos,
        Coder<?> coder) {
      this.reader = reader;
      this.firstPage = firstPage;
      this.secondPagePos = secondPagePos;
      this.coder = coder;
    }

    @Override
    public Iterator<ResultT> iterator() {
      return new AbstractIterator<ResultT>() {
        private Iterator<ResultT> currentPage = firstPage.iterator();
        private StateTag<ContinuationT> nextPagePos = secondPagePos;
        private Future<ValuesAndContPosition<ResultT, ContinuationT>> pendingNextPage =
            // NOTE: The results of continuation page reads are never cached.
            reader.continuationFuture(nextPagePos, coder);

        @Override
        protected ResultT computeNext() {
          while (true) {
            if (currentPage.hasNext()) {
              return currentPage.next();
            }
            if (pendingNextPage == null) {
              return endOfData();
            }

            ValuesAndContPosition<ResultT, ContinuationT> valuesAndContPosition;
            try {
              valuesAndContPosition = pendingNextPage.get();
            } catch (InterruptedException | ExecutionException e) {
              if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
              }
              throw new RuntimeException("Unable to read value from state", e);
            }
            currentPage = valuesAndContPosition.values.iterator();
            StateTag.Builder<ContinuationT> nextPageBuilder =
                StateTag.of(
                        nextPagePos.getKind(),
                        nextPagePos.getTag(),
                        nextPagePos.getStateFamily(),
                        valuesAndContPosition.continuationPosition)
                    .toBuilder();
            if (secondPagePos.getSortedListRange() != null) {
              nextPageBuilder.setSortedListRange(secondPagePos.getSortedListRange());
            }
            if (secondPagePos.getOmitValues() != null) {
              nextPageBuilder.setOmitValues(secondPagePos.getOmitValues());
            }
            if (secondPagePos.getMultimapKey() != null) {
              nextPageBuilder.setMultimapKey(secondPagePos.getMultimapKey());
            }
            nextPagePos = nextPageBuilder.build();
            pendingNextPage =
                // NOTE: The results of continuation page reads are never cached.
                reader.continuationFuture(nextPagePos, coder);
          }
        }
      };
    }
  }
}
