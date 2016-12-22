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
package org.apache.beam.sdk.io.distributedlog;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;

/**
 * The unbounded/bounded source and the sink for <a href="http://distributedlog.io">DistributedLog</a> streams.
 */
public class DistributedLogIO {

  /**
   * Create an uninitialized {@link org.apache.beam.sdk.io.UnboundedSource} builder for distributedlog streams.
   * Before use, basic DistributedLog configuration should set with {@link BoundedSourceBuilder#withNamespace(URI)}
   * and {@link BoundedSourceBuilder#withStreams(List)}.
   *
   * @return an uninitialized {@link org.apache.beam.sdk.io.UnboundedSource} builder for distributedlog streams.
   */
  public static BoundedSourceBuilder<byte[]> unboundedSource() {
    return new BoundedSourceBuilder<>(
        URI.create("distributedlog://127.0.0.1/messaging/distributedlog"),
        Optional.<URI>absent(),
        Lists.<String>newArrayList(),
        ByteArrayCoder.of(),
        null);
  }

  public static class BoundedSourceBuilder<R> {

    protected final URI dlUri;
    protected final Optional<URI> confUri;
    protected final List<String> streams;
    protected final Coder<R> rCoder;
    protected final @Nullable LogSegmentBundle segment;

    private BoundedSourceBuilder(
        URI dlUri,
        Optional<URI> confUri,
        List<String> streams,
        Coder<R> rCoder,
        LogSegmentBundle segment) {
      this.dlUri = dlUri;
      this.confUri = confUri;
      this.streams = streams;
      this.rCoder = rCoder;
      this.segment = segment;
    }

    /**
     * Returns a new {@link BoundedSourceBuilder} pointing to namespace {@code dlUri}.
     *
     * @param dlUri uri of the new distributedlog namespace
     * @return a new {@link BoundedSourceBuilder} pointing to namespace {@code dlUri}.
     */
    public BoundedSourceBuilder<R> withNamespace(URI dlUri) {
      return new BoundedSourceBuilder<>(dlUri, confUri, streams, rCoder, segment);
    }

    /**
     * Returns a new {@link BoundedSourceBuilder} pointing to new configuration {@code confUri}.
     *
     * @param confUri uri of the distributedlog configuration file
     * @return a new {@link BoundedSourceBuilder} pointing to new configuration {@code confUri}.
     */
    public BoundedSourceBuilder<R> withConfiguration(URI confUri) {
      return new BoundedSourceBuilder<>(dlUri, Optional.fromNullable(confUri), streams, rCoder, segment);
    }

    /**
     * Returns a new {@link BoundedSourceBuilder} pointing to new list of {@code streams}.
     *
     * @param streams new list of distributedlog streams
     * @return a new {@link BoundedSourceBuilder} pointing to new list of {@code streams}.
     */
    public BoundedSourceBuilder<R> withStreams(List<String> streams) {
      return new BoundedSourceBuilder<>(dlUri, confUri, streams, rCoder, segment);
    }

    /**
     * Returns a new {@link BoundedSourceBuilder} using new record {@code coder}.
     *
     * @param coder record coder
     * @return a new {@link BoundedSourceBuilder} using new record {@code coder}.
     */
    public <RT> BoundedSourceBuilder<RT> withRecordCoder(Coder<RT> coder) {
      return new BoundedSourceBuilder<>(dlUri, confUri, streams, coder, segment);
    }

    /**
     * Build a initialized {@link BoundedSource} for distributedlog streams.
     *
     * @return the initialized {@link BoundedSource} for distributedlog streams.
     */
    public BoundedSource<R> build() {
      return new DLBoundedSource<>(
          dlUri,
          confUri,
          streams,
          rCoder,
          segment);
    }

  }

}
