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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.commons.configuration.ConfigurationException;

/**
 * A {@code BoundedSource} for reading log streams resident in a DistributedLog cluster.
 */
class DLBoundedSource<R> extends BoundedSource<R> {

  private static final long serialVersionUID = 3551126210922557330L;

  protected final URI dlUri;
  protected final Optional<URI> confUri;
  protected final List<String> streams;
  protected final Coder<R> rCoder;
  protected final @Nullable LogSegmentBundle segment;

  DLBoundedSource(
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

  DistributedLogNamespace setupNamespace() throws IOException {
    DistributedLogConfiguration conf = new DistributedLogConfiguration();
    if (confUri.isPresent()) {
      try {
        conf.loadConf(confUri.get().toURL());
      } catch (ConfigurationException e) {
        throw new IOException("Failed to load distributedlog configuration from " + confUri, e);
      } catch (MalformedURLException e) {
        throw new IOException("Invalid distributedlog configuration uri " + confUri, e);
      }
    }
    return DistributedLogNamespaceBuilder.newBuilder()
        .conf(conf)
        .uri(dlUri)
        .clientId("distributedlog-bounded-source-" + InetAddress.getLocalHost().getHostName())
        .statsLogger(NullStatsLogger.INSTANCE)
        .regionId(DistributedLogConstants.LOCAL_REGION_ID)
        .build();
  }

  List<String> getStreams() {
    return streams;
  }

  @Override
  public void validate() {
    checkArgument(!streams.isEmpty(), "need to set the streams of a DLBoundedSource");
    checkNotNull(rCoder, "need to set the key coder of a DLBoundedSource");
  }

  static List<LogSegmentBundle> getAllLogSegments(
      DistributedLogNamespace namespace,
      List<String> streams) throws IOException {
    List<LogSegmentBundle> segments = Lists.newArrayList();
    for (String stream : streams) {
      segments.addAll(getLogSegments(namespace, stream));
    }
    return segments;
  }

  private static List<LogSegmentBundle> getLogSegments(
      DistributedLogNamespace namespace,
      final String stream) throws IOException {
    DistributedLogManager manager = namespace.openLog(stream);
    try {
      return Lists.transform(manager.getLogSegments(),
          new Function<LogSegmentMetadata, LogSegmentBundle>() {
            @Nullable
            @Override
            public LogSegmentBundle apply(@Nullable LogSegmentMetadata metadata) {
              return new LogSegmentBundle(stream, metadata);
            }
          });
    } finally {
      manager.close();
    }
  }

  @Override
  public List<? extends BoundedSource<R>> splitIntoBundles(long desiredBundleSizeBytes,
                                                           PipelineOptions options) throws Exception {
    DistributedLogNamespace namespace = setupNamespace();
    try {
      if (null == segment) {
        return Lists.transform(getAllLogSegments(namespace, streams),
            new Function<LogSegmentBundle, BoundedSource<R>>() {
              @Override
              public BoundedSource<R> apply(@Nullable LogSegmentBundle bundle) {
                return new DLBoundedSource<>(
                    dlUri,
                    confUri,
                    streams,
                    rCoder,
                    bundle);
              }
            });
      } else {
        return ImmutableList.of(this);
      }
    } finally {
      namespace.close();
    }
  }

  long getLength(LogSegmentMetadata metadata) {
    return Math.max(0L, metadata.getLastEntryId());
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    if (null != segment) {
      return getLength(segment.getMetadata());
    }
    long size = 0;
    DistributedLogNamespace namespace = setupNamespace();
    try {
      List<LogSegmentBundle> segments = getAllLogSegments(namespace, streams);
      for (LogSegmentBundle segment : segments) {
        size += getLength(segment.getMetadata());
      }
      return size;
    } finally {
      namespace.close();
    }
  }

  @Override
  public boolean producesSortedKeys(PipelineOptions options) throws Exception {
    return false;
  }

  @Override
  public BoundedReader<R> createReader(PipelineOptions options) throws IOException {
    this.validate();
    return new MultipleSegmentsReader<>(this, segment);
  }

  @Override
  public Coder<R> getDefaultOutputCoder() {
    return rCoder;
  }
}
