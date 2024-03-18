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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WindmillBag<T> extends SimpleWindmillState implements BagState<T> {

  private final StateNamespace namespace;
  private final StateTag<BagState<T>> address;
  private final ByteString stateKey;
  private final String stateFamily;
  private final Coder<T> elemCoder;

  private boolean cleared = false;
  /**
   * If non-{@literal null}, this contains the complete contents of the bag, except for any local
   * additions. If {@literal null} then we don't know if Windmill contains additional values which
   * should be part of the bag. We'll need to read them if the work item actually wants the bag
   * contents.
   */
  private ConcatIterables<T> cachedValues = null;

  private List<T> localAdditions = new ArrayList<>();
  private long encodedSize = 0;

  WindmillBag(
      StateNamespace namespace,
      StateTag<BagState<T>> address,
      String stateFamily,
      Coder<T> elemCoder,
      boolean isNewKey) {
    this.namespace = namespace;
    this.address = address;
    this.stateKey = WindmillStateUtil.encodeKey(namespace, address);
    this.stateFamily = stateFamily;
    this.elemCoder = elemCoder;
    if (isNewKey) {
      this.cachedValues = new ConcatIterables<>();
    }
  }

  @Override
  public void clear() {
    cleared = true;
    cachedValues = new ConcatIterables<>();
    localAdditions = new ArrayList<>();
    encodedSize = 0;
  }

  /**
   * Return iterable over all bag values in Windmill which should contribute to overall bag
   * contents.
   */
  private Iterable<T> fetchData(Future<Iterable<T>> persistedData) {
    try (Closeable scope = scopedReadState()) {
      if (cachedValues != null) {
        return cachedValues.snapshot();
      }
      Iterable<T> data = persistedData.get();
      if (data instanceof Weighted) {
        // We have a known bounded amount of data; cache it.
        cachedValues = new ConcatIterables<>();
        cachedValues.extendWith(data);
        encodedSize = ((Weighted) data).getWeight();
        return cachedValues.snapshot();
      } else {
        // This is an iterable that may not fit in memory at once; don't cache it.
        return data;
      }
    } catch (InterruptedException | ExecutionException | IOException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Unable to read state", e);
    }
  }

  public boolean valuesAreCached() {
    return cachedValues != null;
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public WindmillBag<T> readLater() {
    getFuture();
    return this;
  }

  @Override
  public Iterable<T> read() {
    return Iterables.concat(
        fetchData(getFuture()), Iterables.limit(localAdditions, localAdditions.size()));
  }

  @Override
  public ReadableState<Boolean> isEmpty() {
    return new ReadableState<Boolean>() {
      @Override
      public ReadableState<Boolean> readLater() {
        WindmillBag.this.readLater();
        return this;
      }

      @Override
      public Boolean read() {
        return Iterables.isEmpty(fetchData(getFuture())) && localAdditions.isEmpty();
      }
    };
  }

  @Override
  public void add(T input) {
    localAdditions.add(input);
  }

  @Override
  public Windmill.WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKeyAndFamily cache)
      throws IOException {
    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();

    Windmill.TagBag.Builder bagUpdatesBuilder = null;

    if (cleared) {
      bagUpdatesBuilder = commitBuilder.addBagUpdatesBuilder();
      bagUpdatesBuilder.setDeleteAll(true);
      cleared = false;
    }

    if (!localAdditions.isEmpty()) {
      // Tell Windmill to capture the local additions.
      if (bagUpdatesBuilder == null) {
        bagUpdatesBuilder = commitBuilder.addBagUpdatesBuilder();
      }
      for (T value : localAdditions) {
        ByteStringOutputStream stream = new ByteStringOutputStream();
        // Encode the value
        elemCoder.encode(value, stream, Coder.Context.OUTER);
        ByteString encoded = stream.toByteString();
        if (cachedValues != null) {
          // We'll capture this value in the cache below.
          // Capture the value's size now since we have it.
          encodedSize += encoded.size();
        }
        bagUpdatesBuilder.addValues(encoded);
      }
    }

    if (bagUpdatesBuilder != null) {
      bagUpdatesBuilder.setTag(stateKey).setStateFamily(stateFamily);
    }

    if (cachedValues != null) {
      if (!localAdditions.isEmpty()) {
        // Capture the local additions in the cached value since we and
        // Windmill are now in agreement.
        cachedValues.extendWith(localAdditions);
      }
      // We now know the complete bag contents, and any read on it will yield a
      // cached value, so cache it for future reads.
      cache.put(namespace, address, this, encodedSize + stateKey.size());
    }

    // Don't reuse the localAdditions object; we don't want future changes to it to
    // modify the value of cachedValues.
    localAdditions = new ArrayList<>();

    return commitBuilder.buildPartial();
  }

  private Future<Iterable<T>> getFuture() {
    return cachedValues != null ? null : reader.bagFuture(stateKey, stateFamily, elemCoder);
  }
}
