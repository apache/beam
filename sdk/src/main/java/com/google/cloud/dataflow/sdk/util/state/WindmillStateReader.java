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
package com.google.cloud.dataflow.sdk.util.state;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.runners.worker.MetricTrackingWindmillServerStub;
import com.google.cloud.dataflow.sdk.runners.worker.StreamingDataflowWorker;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.TagList;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.TagValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ForwardingFuture;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reads persistent state from {@link Windmill}. Returns {@code Future}s containing the data that
 * has been read. Will not initiate a read until {@link Future#get} is called, at which point all
 * the pending futures will be read.
 */
public class WindmillStateReader {

  private static class StateTag {
    private enum Kind {
      VALUE,
      LIST,
      WATERMARK;
    }

    private final Kind kind;
    private final ByteString tag;
    private final String stateFamily;

    private StateTag(Kind kind, ByteString tag, String stateFamily) {
      this.kind = kind;
      this.tag = tag;
      this.stateFamily = Preconditions.checkNotNull(stateFamily);
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
      return Objects.equal(this.kind, that.kind)
          && Objects.equal(this.tag, that.tag)
          && Objects.equal(this.stateFamily, that.stateFamily);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(kind, tag, stateFamily);
    }

    @Override
    public String toString() {
      return "Tag(" + kind + "," + tag.toStringUtf8() + "," + stateFamily + ")";
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(WindmillStateReader.class);

  private final String computation;
  private final ByteString key;
  private final long workToken;

  private final MetricTrackingWindmillServerStub metrics;

  public WindmillStateReader(
      MetricTrackingWindmillServerStub metrics,
      String computation, ByteString key, long workToken) {
    this.metrics = metrics;
    this.computation = computation;
    this.key = key;
    this.workToken = workToken;
  }

  @VisibleForTesting ConcurrentLinkedQueue<StateTag> pendingLookups = new ConcurrentLinkedQueue<>();
  private ConcurrentHashMap<StateTag, Coder<?>> coders = new ConcurrentHashMap<>();

  private ConcurrentHashMap<StateTag, SettableFuture<?>> futures = new ConcurrentHashMap<>();

  private <T> Future<T> stateFuture(StateTag tag, Coder<?> coder) {
    SettableFuture<?> wildcardFuture = futures.get(tag);
    if (wildcardFuture == null) {
      // If we don't yet have a future, try to create one.
      wildcardFuture = SettableFuture.<T>create();
      SettableFuture<?> old = futures.putIfAbsent(tag, wildcardFuture);

      if (old == null) {
        // We won the race, queue the lookup and coder.
        pendingLookups.add(tag);
        if (coder != null) {
          coders.putIfAbsent(tag, coder);
        }
      } else {
        // We lost the race, use the other future.
        wildcardFuture = old;
      }
    }

    @SuppressWarnings("unchecked")
    SettableFuture<T> typedFuture = (SettableFuture<T>) wildcardFuture;
    return wrappedFuture(typedFuture);
  }

  public Future<Instant> watermarkFuture(ByteString encodedTag, String stateFamily) {
    return stateFuture(new StateTag(StateTag.Kind.WATERMARK, encodedTag, stateFamily), null);
  }

  public <T> Future<T> valueFuture(ByteString encodedTag, String stateFamily, Coder<T> coder) {
    return stateFuture(new StateTag(StateTag.Kind.VALUE, encodedTag, stateFamily), coder);
  }

  public <T> Future<Iterable<T>> listFuture(ByteString encodedTag, String stateFamily,
      Coder<T> elemCoder) {
    return stateFuture(new StateTag(StateTag.Kind.LIST, encodedTag, stateFamily), elemCoder);
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

  public void startBatchAndBlock() {
    // First, drain work out of the pending lookups into a set. These will be the items we fetch.
    HashSet<StateTag> toFetch = new HashSet<>();
    while (!pendingLookups.isEmpty()) {
      StateTag tag = pendingLookups.poll();
      if (tag == null) {
        break;
      }

      if (!toFetch.add(tag)) {
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

  private Windmill.GetDataRequest createRequest(Iterable<StateTag> toFetch) {
    Windmill.GetDataRequest.Builder request = Windmill.GetDataRequest.newBuilder();
    Windmill.KeyedGetDataRequest.Builder keyedDataBuilder = request
        .addRequestsBuilder().setComputationId(computation)
        .addRequestsBuilder().setKey(key).setWorkToken(workToken);

    for (StateTag tag : toFetch) {
      switch (tag.kind) {
        case LIST:
          keyedDataBuilder
              .addListsToFetchBuilder()
              .setTag(tag.tag)
              .setStateFamily(tag.stateFamily)
              .setEndTimestamp(Long.MAX_VALUE);
          break;

        case WATERMARK:
          keyedDataBuilder
              .addWatermarkHoldsToFetchBuilder()
              .setTag(tag.tag)
              .setStateFamily(tag.stateFamily);
          break;

        case VALUE:
          keyedDataBuilder
              .addValuesToFetchBuilder()
              .setTag(tag.tag)
              .setStateFamily(tag.stateFamily);
          break;

        default:
          throw new RuntimeException("Unknown kind of tag requested: " + tag.kind);
      }
    }

    return request.build();
  }

  private void consumeResponse(Windmill.GetDataRequest request,
      Windmill.GetDataResponse getDataResponse, Set<StateTag> toFetch) {
    // Validate the response is for our computation/key.
    if (getDataResponse.getDataCount() == 0) {
      throw new RuntimeException(
          "No computation in response to request: " + request);
    } else if (getDataResponse.getDataCount() > 1) {
      throw new RuntimeException("Expected exactly one computation in response, but got: "
          + getDataResponse.getDataList());
    }

    Windmill.ComputationGetDataResponse computationResponse = getDataResponse.getData(0);

    if (!computation.equals(computationResponse.getComputationId())) {
      throw new RuntimeException("Expected data for computation " + computation
          + " but was " + computationResponse.getComputationId());
    }

    if (computationResponse.getDataCount() == 0) {
      throw new RuntimeException(
          "No key in response to request: " + request);
    } else if (computationResponse.getDataCount() > 1) {
      throw new RuntimeException(
          "Expected exactly one key in response, but was: " + computationResponse.getDataList());
    }

    Windmill.KeyedGetDataResponse response = computationResponse.getData(0);

    if (response.getFailed()) {
      // Set up all the futures for this key to throw an exception:
      StreamingDataflowWorker.KeyTokenInvalidException keyTokenInvalidException =
          new StreamingDataflowWorker.KeyTokenInvalidException(key.toStringUtf8());
      for (StateTag stateTag : toFetch) {
        futures.get(stateTag).setException(keyTokenInvalidException);
      }
      return;
    }

    if (!key.equals(response.getKey())) {
      throw new RuntimeException("Expected data for key " + key
          + " but was " + response.getKey());
    }


    for (Windmill.TagList list : response.getListsList()) {
      StateTag stateTag = new StateTag(
          StateTag.Kind.LIST, list.getTag(), list.getStateFamily());
      if (!toFetch.remove(stateTag)) {
        throw new IllegalStateException(
            "Received response for unrequested tag " + stateTag + ". Pending tags: " + toFetch);
      }
      consumeTagList(list, stateTag);
    }

    for (Windmill.WatermarkHold hold : response.getWatermarkHoldsList()) {
      StateTag stateTag = new StateTag(
          StateTag.Kind.WATERMARK, hold.getTag(), hold.getStateFamily());
      if (!toFetch.remove(stateTag)) {
        throw new IllegalStateException(
            "Received response for unrequested tag " + stateTag + ". Pending tags: " + toFetch);
      }
      consumeWatermark(hold, stateTag);
    }

    for (Windmill.TagValue value : response.getValuesList()) {
      StateTag stateTag = new StateTag(
          StateTag.Kind.VALUE, value.getTag(), value.getStateFamily());
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

  private <T> void consumeTagList(TagList list, StateTag stateTag) {
    @SuppressWarnings("unchecked")
    SettableFuture<Iterable<T>> future = (SettableFuture<Iterable<T>>) futures.get(stateTag);
    if (future == null) {
      throw new IllegalStateException("Missing future for " + stateTag);
    } else if (future.isDone()) {
      LOG.error("Future for {} is already done", stateTag);
    }

    if (list.getValuesCount() == 0) {
      future.set(Collections.<T>emptyList());
      return;
    }

    @SuppressWarnings("unchecked")
    Coder<T> elemCoder = (Coder<T>) coders.remove(stateTag);
    if (elemCoder == null) {
      throw new IllegalStateException("Missing element coder for " + stateTag);
    }

    List<T> valueList = new ArrayList<>(list.getValuesCount());
    for (Windmill.Value value : list.getValuesList()) {
      if (value.hasData() && !value.getData().isEmpty()) {
        // Drop the first byte of the data; it's the zero byte we prependend to avoid writing
        // empty data.
        InputStream inputStream = value.getData().substring(1).newInput();
        try {
          valueList.add(elemCoder.decode(inputStream, Coder.Context.OUTER));
        } catch (IOException e) {
          throw new IllegalStateException(
              "Unable to decode tag list using " + elemCoder, e);
        }
      }
    }

    future.set(Collections.unmodifiableList(valueList));
  }

  private void consumeWatermark(Windmill.WatermarkHold watermarkHold, StateTag stateTag) {
    @SuppressWarnings("unchecked")
    SettableFuture<Instant> future = (SettableFuture<Instant>) futures.get(stateTag);
    if (future == null) {
      throw new IllegalStateException("Missing future for " + stateTag);
    } else if (future.isDone()) {
      LOG.error("Future for {} is already done", stateTag);
    }

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
    @SuppressWarnings("unchecked")
    SettableFuture<T> future = (SettableFuture<T>) futures.get(stateTag);
    if (future == null) {
      throw new IllegalStateException("Missing future for " + stateTag);
    } else if (future.isDone()) {
      LOG.error("Future for {} is already done", stateTag);
    }

    @SuppressWarnings("unchecked")
    Coder<T> coder = (Coder<T>) coders.remove(stateTag);
    if (coder == null) {
      throw new IllegalStateException("Missing coder for " + stateTag);
    }

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
}
