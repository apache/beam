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
package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.TagList;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.TagValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.ForwardingFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import javax.annotation.Nullable;

/**
 * Reads persistent state from {@link Windmill}. Returns {@code Future}s containing the data that
 * has been read. Will not initiate a read until {@link Future#get} is called, at which point all
 * the pending futures will be read.
 */
class WindmillStateReader {
  /**
   * Ideal maximum bytes in a TagList response. However, Windmill will always return
   * at least one value if possible irrespective of this limit.
   */
  public static final long MAX_LIST_BYTES = 1L << 20; // 1MB

  /**
   * When combined with a key and computationId, represents the unique address for
   * state managed by Windmill.
   */
  private static class StateTag {
    private enum Kind {
      VALUE,
      LIST,
      WATERMARK;
    }

    private final Kind kind;
    private final ByteString tag;
    private final String stateFamily;

    /**
     * For {@link Kind#LIST} kinds: A previous 'continuation_token' returned by Windmill to signal
     * the resulting list was incomplete. Sending that token will request the next page of values.
     * Null for first request.
     *
     * <p>Null for other kinds.
     */
    @Nullable
    private final ByteString requestToken;

    private StateTag(
        Kind kind, ByteString tag, String stateFamily, @Nullable ByteString requestToken) {
      this.kind = kind;
      this.tag = tag;
      this.stateFamily = Preconditions.checkNotNull(stateFamily);
      this.requestToken = requestToken;
    }

    private StateTag(Kind kind, ByteString tag, String stateFamily) {
      this(kind, tag, stateFamily, null);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof StateTag)) {
        return false;
      }

      StateTag that = (StateTag) obj;
      return Objects.equal(this.kind, that.kind) && Objects.equal(this.tag, that.tag)
          && Objects.equal(this.stateFamily, that.stateFamily)
          && Objects.equal(this.requestToken, that.requestToken);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(kind, tag, stateFamily, requestToken);
    }

    @Override
    public String toString() {
      return "Tag(" + kind + "," + tag.toStringUtf8() + "," + stateFamily
          + (requestToken == null ? "" : ("," + requestToken.toStringUtf8())) + ")";
    }
  }

  /**
   * An in-memory collection of deserialized values and an optional continuation token to pass to
   * Windmill when fetching the next page of values.
   */
  private static class ValuesAndContToken<T> {
    private final List<T> values;

    /** Token to pass to next request for next page of values. Null if done. */
    @Nullable
    private final ByteString continuationToken;

    public ValuesAndContToken(List<T> values, @Nullable ByteString continuationToken) {
      this.values = values;
      this.continuationToken = continuationToken;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(WindmillStateReader.class);

  private final String computation;
  private final ByteString key;
  private final long workToken;

  private final MetricTrackingWindmillServerStub metrics;

  private long bytesRead = 0L;

  public WindmillStateReader(MetricTrackingWindmillServerStub metrics, String computation,
      ByteString key, long workToken) {
    this.metrics = metrics;
    this.computation = computation;
    this.key = key;
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

  @VisibleForTesting
  ConcurrentLinkedQueue<StateTag> pendingLookups = new ConcurrentLinkedQueue<>();
  private ConcurrentHashMap<StateTag, CoderAndFuture<?, ?>> waiting = new ConcurrentHashMap<>();

  private <ElemT, FutureT> Future<FutureT> stateFuture(
      StateTag stateTag, @Nullable Coder<ElemT> coder) {
    CoderAndFuture<ElemT, FutureT> coderAndFuture =
        new CoderAndFuture<ElemT, FutureT>(coder, SettableFuture.<FutureT>create());
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

  public <T> Future<Iterable<T>> listFuture(
      ByteString encodedTag, String stateFamily, Coder<T> elemCoder) {
    // First request has no continuation token.
    StateTag stateTag = new StateTag(StateTag.Kind.LIST, encodedTag, stateFamily);
    // Convert the ValuesAndContToken<T> to Iterable<T>.
    return valuesToPagingIterableFuture(
        stateTag, elemCoder, this.<T, ValuesAndContToken<T>>stateFuture(stateTag, elemCoder));
  }

  /**
   * Internal request to fetch the next 'page' of values in a TagList. Return null if
   * no continuation token is in {@code contTag}, which signals there are no more pages.
   */
  @Nullable
  private <T> Future<ValuesAndContToken<T>> continuationListFuture(
      StateTag contStateTag, Coder<T> elemCoder) {
    if (contStateTag.requestToken == null) {
      // We're done.
      return null;
    }
    return stateFuture(contStateTag, elemCoder);
  }

  private <T> Future<T> wrappedFuture(final Future<T> future) {
    // If the underlying lookup is already complete, we don't need to create the wrapper.
    if (future.isDone()) {
      return future;
    }

    return new ForwardingFuture<T>() {
      @Override
      protected Future<T> delegate() {
        return future;
      }

      @Override
      public T get() throws InterruptedException, ExecutionException {
        if (!future.isDone()) {
          startBatchAndBlock();
        }
        return super.get();
      }

      @Override
      public T get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        if (!future.isDone()) {
          startBatchAndBlock();
        }
        return super.get(timeout, unit);
      }
    };
  }

  /**
   * Return future which transforms a {@code ValuesAndContToken<T>} result into the
   * initial Iterable<T> result expected from the external caller.
   */
  private <T> Future<Iterable<T>> valuesToPagingIterableFuture(final StateTag stateTag,
      final Coder<T> elemCoder, final Future<ValuesAndContToken<T>> future) {
    return Futures.lazyTransform(future, new Function<ValuesAndContToken<T>, Iterable<T>>() {
      @Override
      public Iterable<T> apply(ValuesAndContToken<T> valuesAndContToken) {
        if (valuesAndContToken.continuationToken == null) {
          // Number of values is small enough Windmill sent us the entire list in one response.
          return valuesAndContToken.values;
        } else {
          // Return an iterable which knows how to come back for more.
          StateTag contStateTag = new StateTag(stateTag.kind, stateTag.tag, stateTag.stateFamily,
              valuesAndContToken.continuationToken);
          return new TagListPagingIterable<>(valuesAndContToken.values, contStateTag, elemCoder);
        }
      }
    });
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

    Windmill.GetDataRequest request = createRequest(toFetch);
    Windmill.GetDataResponse response = metrics.getStateData(request);

    if (response == null) {
      throw new RuntimeException("Windmill unexpectedly returned null for request " + request);
    }

    consumeResponse(request, response, toFetch);
  }

  public long getBytesRead() {
    return bytesRead;
  }

  private Windmill.GetDataRequest createRequest(Iterable<StateTag> toFetch) {
    Windmill.GetDataRequest.Builder request = Windmill.GetDataRequest.newBuilder();
    Windmill.KeyedGetDataRequest.Builder keyedDataBuilder =
        request.addRequestsBuilder()
            .setComputationId(computation)
            .addRequestsBuilder()
            .setKey(key)
            .setWorkToken(workToken);

    for (StateTag stateTag : toFetch) {
      switch (stateTag.kind) {
        case LIST:
          TagList.Builder tagList =
              keyedDataBuilder.addListsToFetchBuilder()
                  .setTag(stateTag.tag)
                  .setStateFamily(stateTag.stateFamily)
                  .setEndTimestamp(Long.MAX_VALUE)
                  .setFetchMaxBytes(MAX_LIST_BYTES);
          if (stateTag.requestToken != null) {
            // We're asking for the next page.
            tagList.setRequestToken(stateTag.requestToken);
          }
          break;

        case WATERMARK:
          keyedDataBuilder.addWatermarkHoldsToFetchBuilder()
              .setTag(stateTag.tag)
              .setStateFamily(stateTag.stateFamily);
          break;

        case VALUE:
          keyedDataBuilder.addValuesToFetchBuilder()
              .setTag(stateTag.tag)
              .setStateFamily(stateTag.stateFamily);
          break;

        default:
          throw new RuntimeException("Unknown kind of tag requested: " + stateTag.kind);
      }
    }

    return request.build();
  }

  private void consumeResponse(Windmill.GetDataRequest request,
      Windmill.GetDataResponse getDataResponse, Set<StateTag> toFetch) {
    // Validate the response is for our computation/key.
    if (getDataResponse.getDataCount() == 0) {
      throw new RuntimeException("No computation in response to request: " + request);
    } else if (getDataResponse.getDataCount() > 1) {
      throw new RuntimeException("Expected exactly one computation in response, but got: "
          + getDataResponse.getDataList());
    }

    Windmill.ComputationGetDataResponse computationResponse = getDataResponse.getData(0);

    if (!computation.equals(computationResponse.getComputationId())) {
      throw new RuntimeException("Expected data for computation " + computation + " but was "
          + computationResponse.getComputationId());
    }

    if (computationResponse.getDataCount() == 0) {
      throw new RuntimeException("No key in response to request: " + request);
    } else if (computationResponse.getDataCount() > 1) {
      throw new RuntimeException(
          "Expected exactly one key in response, but was: " + computationResponse.getDataList());
    }

    Windmill.KeyedGetDataResponse response = computationResponse.getData(0);
    bytesRead += response.getSerializedSize();

    if (response.getFailed()) {
      // Set up all the futures for this key to throw an exception:
      StreamingDataflowWorker.KeyTokenInvalidException keyTokenInvalidException =
          new StreamingDataflowWorker.KeyTokenInvalidException(key.toStringUtf8());
      for (StateTag stateTag : toFetch) {
        waiting.get(stateTag).future.setException(keyTokenInvalidException);
      }
      return;
    }

    if (!key.equals(response.getKey())) {
      throw new RuntimeException("Expected data for key " + key + " but was " + response.getKey());
    }


    for (Windmill.TagList tagList : response.getListsList()) {
      StateTag stateTag = new StateTag(StateTag.Kind.LIST, tagList.getTag(),
          tagList.getStateFamily(), tagList.hasRequestToken() ? tagList.getRequestToken() : null);
      if (!toFetch.remove(stateTag)) {
        throw new IllegalStateException(
            "Received response for unrequested tag " + stateTag + ". Pending tags: " + toFetch);
      }
      consumeTagList(tagList, stateTag);
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

  /**
   * The deserialized values in {@code tagList} as a read-only array list.
   */
  private <T> List<T> tagListPageValues(TagList tagList, Coder<T> elemCoder) {
    if (tagList.getValuesCount() == 0) {
      return Collections.<T>emptyList();
    }

    List<T> valueList = new ArrayList<>(tagList.getValuesCount());
    for (Windmill.Value value : tagList.getValuesList()) {
      if (value.hasData() && !value.getData().isEmpty()) {
        // Drop the first byte of the data; it's the zero byte we prepended to avoid writing
        // empty data.
        InputStream inputStream = value.getData().substring(1).newInput();
        try {
          valueList.add(elemCoder.decode(inputStream, Coder.Context.OUTER));
        } catch (IOException e) {
          throw new IllegalStateException("Unable to decode tag list using " + elemCoder, e);
        }
      }
    }
    return Collections.unmodifiableList(valueList);
  }

  private <T> void consumeTagList(TagList tagList, StateTag stateTag) {
    boolean shouldRemove;
    if (stateTag.requestToken == null) {
      // This is the response for the first page.
      // Leave the future in the cache so subsequent requests for the first page
      // can return immediately.
      shouldRemove = false;
    } else {
      // This is a response for a subsequent page.
      // Don't cache the future since we may need to make multiple requests with different
      // continuation tokens.
      shouldRemove = true;
    }
    CoderAndFuture<T, ValuesAndContToken<T>> coderAndFuture = getWaiting(stateTag, shouldRemove);
    SettableFuture<ValuesAndContToken<T>> future = coderAndFuture.getNonDoneFuture(stateTag);
    Coder<T> coder = coderAndFuture.getAndClearCoder();
    List<T> values = this.<T>tagListPageValues(tagList, coder);
    future.set(new ValuesAndContToken<T>(
        values, tagList.hasContinuationToken() ? tagList.getContinuationToken() : null));
  }

  private void consumeWatermark(Windmill.WatermarkHold watermarkHold, StateTag stateTag) {
    CoderAndFuture<Void, Instant> coderAndFuture = getWaiting(stateTag, false);
    SettableFuture<Instant> future = coderAndFuture.getNonDoneFuture(stateTag);
    // No coders for watermarks
    coderAndFuture.checkNoCoder();

    Instant hold = null;
    for (long timestamp : watermarkHold.getTimestampsList()) {
      Instant instant = new Instant(TimeUnit.MICROSECONDS.toMillis(timestamp));
      if (hold == null || instant.isBefore(hold)) {
        hold = instant;
      }
    }

    future.set(hold);
  }

  private <T> void consumeTagValue(TagValue tagValue, StateTag stateTag) {
    CoderAndFuture<T, T> coderAndFuture = getWaiting(stateTag, false);
    SettableFuture<T> future = coderAndFuture.getNonDoneFuture(stateTag);
    Coder<T> coder = coderAndFuture.getAndClearCoder();

    if (tagValue.hasValue() && tagValue.getValue().hasData()
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
   * An iterable over elements backed by paginated GetData requests to Windmill. The
   * iterable may be iterated over an arbitrary number of times and multiple iterators
   * may be active simultaneously.
   *
   * <p>There are two pattern we wish to support with low -memory and -latency:
   * <ol>
   * <li>Re-iterate over the initial elements multiple times (eg Iterables.first). We'll cache
   * the initial 'page' of values returned by Windmill from our first request for the lifetime
   * of the iterable.
   * <li>Iterate through all elements of a very large collection. We'll send the GetData request
   * for the next page when the current page is begun. We'll discard intermediate pages and only
   * retain the first. Thus the maximum memory pressure is one page plus one page per call to
   * iterator.
   * </ol>
   */
  private class TagListPagingIterable<T> implements Iterable<T> {
    /** Initial values returned for the first page. Never reclaimed. */
    private final List<T> firstPage;

    /** State tag with continuation token set for second page. */
    private final StateTag secondPageCont;

    /** Coder for elements. */
    private final Coder<T> elemCoder;

    private TagListPagingIterable(List<T> firstPage, StateTag secondPageCont, Coder<T> elemCoder) {
      this.firstPage = firstPage;
      this.secondPageCont = secondPageCont;
      this.elemCoder = elemCoder;
    }

    @Override
    public Iterator<T> iterator() {
      return new AbstractIterator<T>() {
        private int numPagesRead = 1;
        private Iterator<T> currentPage = firstPage.iterator();
        private StateTag nextPageCont = secondPageCont;
        private Future<ValuesAndContToken<T>> pendingNextPage =
            continuationListFuture(nextPageCont, elemCoder);

        @Override
        protected T computeNext() {
          while (true) {
            if (currentPage.hasNext()) {
              return currentPage.next();
            }
            if (pendingNextPage == null) {
              return endOfData();
            }

            ValuesAndContToken<T> valuesAndContToken;
            try {
              valuesAndContToken = pendingNextPage.get();
              numPagesRead++;
            } catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException("Unable to read value from state", e);
            }
            currentPage = valuesAndContToken.values.iterator();
            nextPageCont = new StateTag(nextPageCont.kind, nextPageCont.tag,
                nextPageCont.stateFamily, valuesAndContToken.continuationToken);
            pendingNextPage = continuationListFuture(nextPageCont, elemCoder);
          }
        }
      };
    }
  }
}
