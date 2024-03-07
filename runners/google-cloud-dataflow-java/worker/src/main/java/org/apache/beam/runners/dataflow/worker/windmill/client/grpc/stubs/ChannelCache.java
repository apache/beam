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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache of gRPC channels for {@link WindmillServiceAddress}. Follows <a
 * href=https://grpc.io/docs/guides/performance/#java>gRPC recommendations</a> for re-using channels
 * when possible.
 *
 * @implNote Backed by {@link LoadingCache} which is thread-safe.
 */
@ThreadSafe
public final class ChannelCache implements StatusDataProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ChannelCache.class);
  private final LoadingCache<WindmillServiceAddress, ManagedChannel> channelCache;

  public ChannelCache(
      boolean useIsolatedChannels,
      Function<WindmillServiceAddress, ManagedChannel> channelFactory,
      RemovalListener<WindmillServiceAddress, ManagedChannel> onChannelRemoved) {
    this.channelCache =
        Caffeine.newBuilder()
            .removalListener(onChannelRemoved)
            .build(
                serviceAddress ->
                    // IsolationChannel will create and manage separate RPC channels to the same
                    // serviceAddress via calling the channelFactory, else just directly return the
                    // RPC channel.
                    useIsolatedChannels
                        ? IsolationChannel.create(() -> channelFactory.apply(serviceAddress))
                        : channelFactory.apply(serviceAddress));
  }

  private static void shutdownChannel(ManagedChannel channel) {
    channel.shutdown();
    try {
      channel.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Couldn't close gRPC channel={}", channel, e);
    }
    channel.shutdownNow();
  }

  public static ChannelCache create(
      boolean useIsolatedChannels,
      Function<WindmillServiceAddress, ManagedChannel> channelFactory) {
    return new ChannelCache(
        useIsolatedChannels,
        channelFactory,
        // Shutdown the channels as they get removed from the cache, so they do not leak.
        (address, channel, cause) -> shutdownChannel(channel));
  }

  @VisibleForTesting
  static ChannelCache forTesting(
      boolean useIsolatedChannels,
      Function<WindmillServiceAddress, ManagedChannel> channelFactory,
      Runnable onChannelShutdown) {
    return new ChannelCache(
        useIsolatedChannels,
        channelFactory,
        // Shutdown the channels as they get removed from the cache, so they do not leak.
        // For testing so that we don't have to sleep/wait for arbitrary time in test.
        (address, channel, cause) -> {
          shutdownChannel(channel);
          onChannelShutdown.run();
        });
  }

  public ManagedChannel get(WindmillServiceAddress windmillServiceAddress) {
    return channelCache.get(windmillServiceAddress);
  }

  public void remove(WindmillServiceAddress windmillServiceAddress) {
    channelCache.invalidate(windmillServiceAddress);
  }

  public void clear() {
    channelCache.invalidateAll();
  }

  @VisibleForTesting
  boolean isEmpty() {
    // Perform any pending removal/insert operations first.
    channelCache.cleanUp();
    return channelCache.estimatedSize() == 0;
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    writer.write("Active gRPC Channels:<br>");
    for (Map.Entry<WindmillServiceAddress, ManagedChannel> addressAndChannel :
        channelCache.asMap().entrySet()) {
      writer.format(
          "Address: [%s]; Channel: [%s]; ChannelState: [%s]",
          addressAndChannel.getKey(),
          addressAndChannel.getValue(),
          addressAndChannel.getValue().getState(false));
      writer.write("<br>");
    }
  }
}
