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

import static org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateUtil.encodeKey;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Futures;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WindmillValue<T> extends SimpleWindmillState implements ValueState<T> {
  private final StateNamespace namespace;
  private final StateTag<ValueState<T>> address;
  private final ByteString stateKey;
  private final String stateFamily;
  private final Coder<T> coder;

  /** Whether we've modified the value since creation of this state. */
  private boolean modified = false;
  /** Whether the in memory value is the true value. */
  private boolean valueIsKnown = false;
  /** The size of the encoded value */
  private long cachedSize = -1;

  private T value;

  WindmillValue(
      StateNamespace namespace,
      StateTag<ValueState<T>> address,
      String stateFamily,
      Coder<T> coder,
      boolean isNewKey) {
    this.namespace = namespace;
    this.address = address;
    this.stateKey = encodeKey(namespace, address);
    this.stateFamily = stateFamily;
    this.coder = coder;
    if (isNewKey) {
      this.valueIsKnown = true;
      this.value = null;
    }
  }

  @Override
  public void clear() {
    modified = true;
    valueIsKnown = true;
    value = null;
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public WindmillValue<T> readLater() {
    getFuture();
    return this;
  }

  @Override
  public T read() {
    try (Closeable scope = scopedReadState()) {
      if (!valueIsKnown) {
        cachedSize = -1;
      }
      value = getFuture().get();
      valueIsKnown = true;
      return value;
    } catch (InterruptedException | ExecutionException | IOException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Unable to read value from state", e);
    }
  }

  @Override
  public void write(T value) {
    modified = true;
    valueIsKnown = true;
    cachedSize = -1;
    this.value = value;
  }

  @Override
  protected Windmill.WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKeyAndFamily cache)
      throws IOException {
    if (!valueIsKnown) {
      // The value was never read, written or cleared.
      // Thus nothing to update in Windmill.
      // And no need to add to global cache.
      return Windmill.WorkItemCommitRequest.newBuilder().buildPartial();
    }

    ByteString encoded = null;
    if (cachedSize == -1 || modified) {
      ByteStringOutputStream stream = new ByteStringOutputStream();
      if (value != null) {
        coder.encode(value, stream, Coder.Context.OUTER);
      }
      encoded = stream.toByteString();
      cachedSize = (long) encoded.size() + stateKey.size();
    }

    // Place in cache to avoid a future read.
    cache.put(namespace, address, this, cachedSize);

    if (!modified) {
      // The value was read, but never written or cleared.
      // But nothing to update in Windmill.
      return Windmill.WorkItemCommitRequest.newBuilder().buildPartial();
    }

    // The value was written or cleared. Commit that change to Windmill.
    modified = false;
    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    commitBuilder
        .addValueUpdatesBuilder()
        .setTag(stateKey)
        .setStateFamily(stateFamily)
        .getValueBuilder()
        .setData(encoded)
        .setTimestamp(Long.MAX_VALUE);
    return commitBuilder.buildPartial();
  }

  private Future<T> getFuture() {
    // WindmillStateReader guarantees that we can ask for a future for a particular tag multiple
    // times and it will efficiently be reused.
    return valueIsKnown
        ? Futures.immediateFuture(value)
        : reader.valueFuture(stateKey, stateFamily, coder);
  }
}
