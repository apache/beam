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
package org.apache.beam.sdk.io.aws2.common;

import static org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory.buildClient;

import java.util.function.Function;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.function.ThrowingConsumer;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashBiMap;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.SdkClient;

/**
 * Reference counting object pool to easily share & destroy objects.
 *
 * <p>Internal only, subject to incompatible changes or removal at any time!
 *
 * <p>NOTE: This relies heavily on the implementation of {@link #equals(Object)} for {@link KeyT}.
 * If not implemented properly, clients can't be shared between instances of {@link
 * org.apache.beam.sdk.transforms.DoFn}.
 *
 * @param <KeyT>> Key to share objects by
 * @param <ObjectT>> Shared object
 */
@Internal
public class ObjectPool<KeyT extends @NonNull Object, ObjectT extends @NonNull Object> {
  private final BiMap<KeyT, RefCounted> pool = HashBiMap.create(2);
  private final Function<KeyT, ObjectT> builder;
  private final @Nullable ThrowingConsumer<Exception, ObjectT> finalizer;

  public ObjectPool(Function<KeyT, ObjectT> builder) {
    this(builder, null);
  }

  public ObjectPool(
      Function<KeyT, ObjectT> builder, @Nullable ThrowingConsumer<Exception, ObjectT> finalizer) {
    this.builder = builder;
    this.finalizer = finalizer;
  }

  /** Retain a reference to a shared client instance. If not available, an instance is created. */
  public ObjectT retain(KeyT key) {
    synchronized (pool) {
      RefCounted ref = pool.computeIfAbsent(key, k -> new RefCounted(builder.apply(k)));
      ref.count++;
      return ref.shared;
    }
  }

  /**
   * Release a reference to a shared object instance using {@link KeyT}. If that instance is not
   * used anymore, it will be removed and destroyed.
   */
  public void releaseByKey(KeyT key) {
    RefCounted ref;
    synchronized (pool) {
      ref = pool.get(key);
      if (ref == null || --ref.count > 0) {
        return;
      }
      pool.remove(key);
    }
    if (finalizer != null) {
      try {
        finalizer.accept(ref.shared);
      } catch (Exception e) {
        LoggerFactory.getLogger(ObjectPool.class).warn("Exception destroying pooled object.", e);
      }
    }
  }

  /**
   * Release a reference to a shared client instance. If that instance is not used anymore, it will
   * be removed and destroyed.
   */
  public void release(ObjectT object) {
    KeyT key = pool.inverse().get(new RefCounted(object));
    if (key != null) {
      releaseByKey(key);
    }
  }

  public static <ClientT extends SdkClient, BuilderT extends AwsClientBuilder<BuilderT, ClientT>>
      ClientPool<ClientT> pooledClientFactory(BuilderT builder) {
    return new ClientPool<>(c -> buildClient(c.getLeft(), builder, c.getRight()));
  }

  /** Client pool to easily share AWS clients per configuration. */
  public static class ClientPool<ClientT extends SdkClient>
      extends ObjectPool<Pair<AwsOptions, ClientConfiguration>, ClientT> {

    private ClientPool(Function<Pair<AwsOptions, ClientConfiguration>, ClientT> builder) {
      super(builder, c -> c.close());
    }

    /** Retain a reference to a shared client instance. If not available, an instance is created. */
    public ClientT retain(AwsOptions provider, ClientConfiguration config) {
      return retain(Pair.of(provider, config));
    }
  }

  private class RefCounted {
    private int count = 0;
    private final ObjectT shared;

    RefCounted(ObjectT client) {
      this.shared = client;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      // only identity of ref counted shared object matters
      return shared == ((RefCounted) o).shared;
    }

    @Override
    public int hashCode() {
      return shared.hashCode();
    }
  }
}
