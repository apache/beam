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

import java.util.function.BiFunction;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBiMap;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;

/**
 * Reference counting pool to easily share AWS clients or similar by individual client provider and
 * configuration (optional).
 *
 * <p>NOTE: This relies heavily on the implementation of {@link #equals(Object)} for {@link
 * ProviderT} and {@link ConfigT}. If not implemented properly, clients can't be shared between
 * instances of {@link org.apache.beam.sdk.transforms.DoFn}.
 *
 * @param <ProviderT> Client provider
 * @param <ConfigT> Optional, nullable configuration
 * @param <ClientT> Client
 */
public class ClientPool<ProviderT, ConfigT, ClientT extends AutoCloseable> {
  private final BiMap<Pair<ProviderT, ConfigT>, RefCounted> pool = HashBiMap.create(2);
  private final BiFunction<ProviderT, ConfigT, ClientT> builder;

  public static <
          ClientT extends AutoCloseable, BuilderT extends AwsClientBuilder<BuilderT, ClientT>>
      ClientPool<AwsOptions, ClientConfiguration, ClientT> pooledClientFactory(BuilderT builder) {
    return new ClientPool<>((opts, conf) -> ClientBuilderFactory.buildClient(opts, builder, conf));
  }

  public ClientPool(BiFunction<ProviderT, ConfigT, ClientT> builder) {
    this.builder = builder;
  }

  /** Retain a reference to a shared client instance. If not available, an instance is created. */
  public ClientT retain(ProviderT provider, @Nullable ConfigT config) {
    @SuppressWarnings("nullness")
    Pair<ProviderT, ConfigT> key = Pair.of(provider, config);
    synchronized (pool) {
      RefCounted ref = pool.computeIfAbsent(key, RefCounted::new);
      ref.count++;
      return ref.client;
    }
  }

  /**
   * Release a reference to a shared client instance using {@link ProviderT} and {@link ConfigT} .
   * If that instance is not used anymore, it will be removed and destroyed.
   */
  public void release(ProviderT provider, @Nullable ConfigT config) throws Exception {
    @SuppressWarnings("nullness")
    Pair<ProviderT, ConfigT> key = Pair.of(provider, config);
    RefCounted ref;
    synchronized (pool) {
      ref = pool.get(key);
      if (ref == null || --ref.count > 0) {
        return;
      }
      pool.remove(key);
    }
    ref.client.close();
  }

  /**
   * Release a reference to a shared client instance. If that instance is not used anymore, it will
   * be removed and destroyed.
   */
  public void release(ClientT client) throws Exception {
    Pair<ProviderT, ConfigT> pair = pool.inverse().get(new RefCounted(client));
    if (pair != null) {
      release(pair.getLeft(), pair.getRight());
    }
  }

  private class RefCounted {
    private int count = 0;
    private final ClientT client;

    RefCounted(ClientT client) {
      this.client = client;
    }

    RefCounted(Pair<ProviderT, ConfigT> key) {
      this(builder.apply(key.getLeft(), key.getRight()));
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      // only identity of ref counted client matters
      return client == ((RefCounted) o).client;
    }

    @Override
    public int hashCode() {
      return client.hashCode();
    }
  }
}
