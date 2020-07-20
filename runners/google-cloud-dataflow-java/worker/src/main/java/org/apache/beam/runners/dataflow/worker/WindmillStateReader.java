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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagBag;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ForwardingList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ForwardingFuture;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.SettableFuture;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Reads persistent state from {@link Windmill}. Returns {@code Future}s containing the data that
 * has been read. Will not initiate a read until {@link Future#get} is called, at which point all
 * the pending futures will be read.
 *
 * <p>CAUTION Watch out for escaping references to the reader ending up inside {@link
 * WindmillStateCache}.
 */
class WindmillStateReader {
  /**
   * Ideal maximum bytes in a TagBag response. However, Windmill will always return at least one
   * value if possible irrespective of this limit.
   */
  public static final long MAX_BAG_BYTES = 8L << 20; // 8MB

  /**
   * Ideal maximum bytes in a KeyedGetDataResponse. However, Windmill will always return at least
   * one value if possible irrespective of this limit.
   */
  public static final long MAX_KEY_BYTES = 16L << 20; // 16MB

  /**
   * When combined with a key and computationId, represents the unique address for state managed by
   * Windmill.
   */
  private static class StateTag {
    private enum Kind {
      VALUE,
      BAG,
      WATERMARK;
    }

    private final Kind kind;
    private final ByteString tag;
    private final String stateFamily;

    /**
     * For {@link Kind#BAG} kinds: A previous 'continuation_position' returned by Windmill to signal
     * the resulting bag was incomplete. Sending that position will request the next page of values.
     * Null for first request.
     *
     * <p>Null for other kinds.
     */
    private final @Nullable Long requestPosition;

    private StateTag(
        Kind kind, ByteString tag, String stateFamily, @Nullable Long requestPosition) {
      this.kind = kind;
      this.tag = tag;
      this.stateFamily = Preconditions.checkNotNull(stateFamily);
      this.requestPosition = requestPosition;
    }

    private StateTag(Kind kind, ByteString tag, String stateFamily) {
      this(kind, tag, stateFamily, null);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof StateTag)) {
        return false;
      }

      StateTag that = (StateTag) obj;
      return Objects.equal(this.kind, that.kind)
          && Objects.equal(this.tag, that.tag)
          && Objects.equal(this.stateFamily, that.stateFamily)
          && Objects.equal(this.requestPosition, that.requestPosition);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(kind, tag, stateFamily, requestPosition);
    }

    @Override
    public String toString() {
      return "Tag("
          + kind
          + ","
          + tag.toStringUtf8()
          + ","
          + stateFamily
          + (requestPosition == null ? "" : ("," + requestPosition.toString()))
          + ")";
    }
  }

  /**
   * An in-memory collection of deserialized values and an optional continuation position to pass to
   * Windmill when fetching the next page of values.
   */
  private static class ValuesAndContPosition<T> {
    private final List<T> values;

    /** Position to pass to next request for next page of values. Null if done. */
    private final @Nullable Long continuationPosition;

    public ValuesAndContPosition(List<T> values, @Nullable Long continuationPosition) {
      this.values = values;
      this.continuationPosition = continuationPosition;
    }
  }

  private final String computation;
  private final ByteString key;
  private final long shardingKey;
  private final long workToken;

  private final MetricTrackingWindmillServerStub server;

  private long bytesRead = 0L;

  public WindmillStateReader(
      MetricTrackingWindmillServerStub server,
      String computation,
      ByteString key,
      long shardingKey,
      long workToken) {
    this.server = server;
    this.computation = computation;
    this.key = key;
    this.shardingKey = shardingKey;
    this.workToken = workToken;
  }

  private static final class CoderAndFuture<ElemT, FutureT> {
    private Coder<ElemT> coder;
    private final SettableFuture<FutureT> future;

    private CoderAndFuture(Coder<ElemT> coder, SettableFuture<FutureT> future) {
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

    private Coder<ElemT> getAndClearCoder() {
      if (coder == null) {
        throw new IllegalStateException("Coder has already been cleared from cache");
      }
      Coder<ElemT> result = coder;
      coder = null;
      return result;
    }

    private void checkNoCoder() {
      if (coder != null) {
        throw new IllegalStateException("Unexpected coder");
      }
    }
  }

  @VisibleForTesting ConcurrentLinkedQueue<StateTag> pendingLookups = new ConcurrentLinkedQueue<>();
  private ConcurrentHashMap<StateTag, CoderAndFuture<?, ?>> waiting = new ConcurrentHashMap<>();

  private <ElemT, FutureT> Future<FutureT> stateFuture(
      StateTag stateTag, @Nullable Coder<ElemT> coder) {
    CoderAndFuture<ElemT, FutureT> coderAndFuture =
        new CoderAndFuture<>(coder, SettableFuture.<FutureT>create());
    CoderAndFuture<?, ?> existingCoderAndFutureWildcard =
        waiting.putIfAbsent(stateTag, coderAndFuture);
    if (existingCoderAndFutureWildcard == null) {
      // Schedule a new request. It's response is guaranteed to find the future and coder.
      pendingLookups.add(stateTag);
    } else {
      // Piggy-back on the pending or already answered request.
      @SuppressWarnings("unchecked")
      CoderAndFuture<ElemT, FutureT> existingCoderAndFuture =
          (CoderAndFuture<ElemT, FutureT>) existingCoderAndFutureWildcard;
      coderAndFuture = existingCoderAndFuture;
    }

    return wrappedFuture(coderAndFuture.getFuture());
  }

  private <ElemT, FutureT> CoderAndFuture<ElemT, FutureT> getWaiting(
      StateTag stateTag, boolean shouldRemove) {
    CoderAndFuture<?, ?> coderAndFutureWildcard;
    if (shouldRemove) {
      coderAndFutureWildcard = waiting.remove(stateTag);
    } else {
      coderAndFutureWildcard = waiting.get(stateTag);
    }
    if (coderAndFutureWildcard == null) {
      throw new IllegalStateException("Missing future for " + stateTag);
    }
    @SuppressWarnings("unchecked")
    CoderAndFuture<ElemT, FutureT> coderAndFuture =
        (CoderAndFuture<ElemT, FutureT>) coderAndFutureWildcard;
    return coderAndFuture;
  }

  public Future<Instant> watermarkFuture(ByteString encodedTag, String stateFamily) {
    return stateFuture(new StateTag(StateTag.Kind.WATERMARK, encodedTag, stateFamily), null);
  }

  public <T> Future<T> valueFuture(ByteString encodedTag, String stateFamily, Coder<T> coder) {
    return stateFuture(new StateTag(StateTag.Kind.VALUE, encodedTag, stateFamily), coder);
  }

  public <T> Future<Iterable<T>> bagFuture(
      ByteString encodedTag, String stateFamily, Coder<T> elemCoder) {
    // First request has no continuation position.
    StateTag stateTag = new StateTag(StateTag.Kind.BAG, encodedTag, stateFamily);
    // Convert the ValuesAndContPosition<T> to Iterable<T>.
    return valuesToPagingIterableFuture(
        stateTag, elemCoder, this.<T, ValuesAndContPosition<T>>stateFuture(stateTag, elemCoder));
  }

  /**
   * Internal request to fetch the next 'page' of values in a TagBag. Return null if no continuation
   * position is in {@code contStateTag}, which signals there are no more pages.
   */
  private @Nullable <T> Future<ValuesAndContPosition<T>> continuationBagFuture(
      StateTag contStateTag, Coder<T> elemCoder) {
    if (contStateTag.requestPosition == null) {
      // We're done.
      return null;
    }
    return stateFuture(contStateTag, elemCoder);
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
  private static class ToIterableFunction<T>
      implements Function<ValuesAndContPosition<T>, Iterable<T>> {
    /**
     * Reader to request continuation pages from, or {@literal null} if no continuation pages
     * required.
     */
    private @Nullable WindmillStateReader reader;

    private final StateTag stateTag;
    private final Coder<T> elemCoder;

    public ToIterableFunction(WindmillStateReader reader, StateTag stateTag, Coder<T> elemCoder) {
      this.reader = reader;
      this.stateTag = stateTag;
      this.elemCoder = elemCoder;
    }

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public Iterable<T> apply(@Nonnull ValuesAndContPosition<T> valuesAndContPosition) {
      if (valuesAndContPosition.continuationPosition == null) {
        // Number of values is small enough Windmill sent us the entire bag in one response.
        reader = null;
        return valuesAndContPosition.values;
      } else {
        // Return an iterable which knows how to come back for more.
        StateTag contStateTag =
            new StateTag(
                stateTag.kind,
                stateTag.tag,
                stateTag.stateFamily,
                valuesAndContPosition.continuationPosition);
        return new BagPagingIterable<>(
            reader, valuesAndContPosition.values, contStateTag, elemCoder);
      }
    }
  }

  /**
   * Return future which transforms a {@code ValuesAndContPosition<T>} result into the initial
   * Iterable<T> result expected from the external caller.
   */
  private <T> Future<Iterable<T>> valuesToPagingIterableFuture(
      final StateTag stateTag,
      final Coder<T> elemCoder,
      final Future<ValuesAndContPosition<T>> future) {
    return Futures.lazyTransform(future, new ToIterableFunction<T>(this, stateTag, elemCoder));
  }

  public void startBatchAndBlock() {
    // First, drain work out of the pending lookups into a set. These will be the items we fetch.
    HashSet<StateTag> toFetch = new HashSet<>();
    while (!pendingLookups.isEmpty()) {
      StateTag stateTag = pendingLookups.poll();
      if (stateTag == null) {
        break;
      }

      if (!toFetch.add(stateTag)) {
        throw new IllegalStateException("Duplicate tags being fetched.");
      }
    }

    // If we failed to drain anything, some other thread pulled it off the queue. We have no work
    // to do.
    if (toFetch.isEmpty()) {
      return;
    }

    Windmill.KeyedGetDataRequest request = createRequest(toFetch);
    Windmill.KeyedGetDataResponse response = server.getStateData(computation, request);

    if (response == null) {
      throw new RuntimeException("Windmill unexpectedly returned null for request " + request);
    }

    consumeResponse(request, response, toFetch);
  }

  public long getBytesRead() {
    return bytesRead;
  }

  private Windmill.KeyedGetDataRequest createRequest(Iterable<StateTag> toFetch) {
    Windmill.KeyedGetDataRequest.Builder keyedDataBuilder =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(key)
            .setShardingKey(shardingKey)
            .setWorkToken(workToken);

    for (StateTag stateTag : toFetch) {
      switch (stateTag.kind) {
        case BAG:
          TagBag.Builder bag =
              keyedDataBuilder
                  .addBagsToFetchBuilder()
                  .setTag(stateTag.tag)
                  .setStateFamily(stateTag.stateFamily)
                  .setFetchMaxBytes(MAX_BAG_BYTES);
          if (stateTag.requestPosition != null) {
            // We're asking for the next page.
            bag.setRequestPosition(stateTag.requestPosition);
          }
          break;

        case WATERMARK:
          keyedDataBuilder
              .addWatermarkHoldsToFetchBuilder()
              .setTag(stateTag.tag)
              .setStateFamily(stateTag.stateFamily);
          break;

        case VALUE:
          keyedDataBuilder
              .addValuesToFetchBuilder()
              .setTag(stateTag.tag)
              .setStateFamily(stateTag.stateFamily);
          break;

        default:
          throw new RuntimeException("Unknown kind of tag requested: " + stateTag.kind);
      }
    }

    keyedDataBuilder.setMaxBytes(MAX_KEY_BYTES);

    return keyedDataBuilder.build();
  }

  private void consumeResponse(
      Windmill.KeyedGetDataRequest request,
      Windmill.KeyedGetDataResponse response,
      Set<StateTag> toFetch) {
    bytesRead += response.getSerializedSize();

    if (response.getFailed()) {
      // Set up all the futures for this key to throw an exception:
      KeyTokenInvalidException keyTokenInvalidException =
          new KeyTokenInvalidException(key.toStringUtf8());
      for (StateTag stateTag : toFetch) {
        waiting.get(stateTag).future.setException(keyTokenInvalidException);
      }
      return;
    }

    if (!key.equals(response.getKey())) {
      throw new RuntimeException("Expected data for key " + key + " but was " + response.getKey());
    }

    for (Windmill.TagBag bag : response.getBagsList()) {
      StateTag stateTag =
          new StateTag(
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
      StateTag stateTag =
          new StateTag(StateTag.Kind.WATERMARK, hold.getTag(), hold.getStateFamily());
      if (!toFetch.remove(stateTag)) {
        throw new IllegalStateException(
            "Received response for unrequested tag " + stateTag + ". Pending tags: " + toFetch);
      }
      consumeWatermark(hold, stateTag);
    }

    for (Windmill.TagValue value : response.getValuesList()) {
      StateTag stateTag = new StateTag(StateTag.Kind.VALUE, value.getTag(), value.getStateFamily());
      if (!toFetch.remove(stateTag)) {
        throw new IllegalStateException(
            "Received response for unrequested tag " + stateTag + ". Pending tags: " + toFetch);
      }
      consumeTagValue(value, stateTag);
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

  private <T> void consumeBag(TagBag bag, StateTag stateTag) {
    boolean shouldRemove;
    if (stateTag.requestPosition == null) {
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
    CoderAndFuture<T, ValuesAndContPosition<T>> coderAndFuture = getWaiting(stateTag, shouldRemove);
    SettableFuture<ValuesAndContPosition<T>> future = coderAndFuture.getNonDoneFuture(stateTag);
    Coder<T> coder = coderAndFuture.getAndClearCoder();
    List<T> values = this.<T>bagPageValues(bag, coder);
    future.set(
        new ValuesAndContPosition<T>(
            values, bag.hasContinuationPosition() ? bag.getContinuationPosition() : null));
  }

  private void consumeWatermark(Windmill.WatermarkHold watermarkHold, StateTag stateTag) {
    CoderAndFuture<Void, Instant> coderAndFuture = getWaiting(stateTag, false);
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

  private <T> void consumeTagValue(TagValue tagValue, StateTag stateTag) {
    CoderAndFuture<T, T> coderAndFuture = getWaiting(stateTag, false);
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
        throw new IllegalStateException("Unable to decode value using " + coder, e);
      }
    } else {
      future.set(null);
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
  private static class BagPagingIterable<T> implements Iterable<T> {
    /**
     * The reader we will use for scheduling continuation pages.
     *
     * <p>NOTE We've made this explicit to remind us to be careful not to cache the iterable.
     */
    private final WindmillStateReader reader;

    /** Initial values returned for the first page. Never reclaimed. */
    private final List<T> firstPage;

    /** State tag with continuation position set for second page. */
    private final StateTag secondPagePos;

    /** Coder for elements. */
    private final Coder<T> elemCoder;

    private BagPagingIterable(
        WindmillStateReader reader, List<T> firstPage, StateTag secondPagePos, Coder<T> elemCoder) {
      this.reader = reader;
      this.firstPage = firstPage;
      this.secondPagePos = secondPagePos;
      this.elemCoder = elemCoder;
    }

    @Override
    public Iterator<T> iterator() {
      return new AbstractIterator<T>() {
        private Iterator<T> currentPage = firstPage.iterator();
        private StateTag nextPagePos = secondPagePos;
        private Future<ValuesAndContPosition<T>> pendingNextPage =
            // NOTE: The results of continuation page reads are never cached.
            reader.continuationBagFuture(nextPagePos, elemCoder);

        @Override
        protected T computeNext() {
          while (true) {
            if (currentPage.hasNext()) {
              return currentPage.next();
            }
            if (pendingNextPage == null) {
              return endOfData();
            }

            ValuesAndContPosition<T> valuesAndContPosition;
            try {
              valuesAndContPosition = pendingNextPage.get();
            } catch (InterruptedException | ExecutionException e) {
              if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
              }
              throw new RuntimeException("Unable to read value from state", e);
            }
            currentPage = valuesAndContPosition.values.iterator();
            nextPagePos =
                new StateTag(
                    nextPagePos.kind,
                    nextPagePos.tag,
                    nextPagePos.stateFamily,
                    valuesAndContPosition.continuationPosition);
            pendingNextPage =
                // NOTE: The results of continuation page reads are never cached.
                reader.continuationBagFuture(nextPagePos, elemCoder);
          }
        }
      };
    }
  }
}
