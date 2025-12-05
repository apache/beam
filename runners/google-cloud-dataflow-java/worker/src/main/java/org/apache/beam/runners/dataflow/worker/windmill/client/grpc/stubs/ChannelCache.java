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

import static org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress.Kind.AUTHENTICATED_GCP_SERVICE_ADDRESS;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.PrintWriter;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.UserWorkerGrpcFlowControlSettings;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalListener;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalListeners;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache of gRPC channels for {@link WindmillServiceAddress}. Follows <a
 * href=https://grpc.io/docs/guides/performance/#java>gRPC recommendations</a> for re-using channels
 * when possible.
 *
 * @implNote Backed by {@link LoadingCache} which is thread-safe. Closes channels when they are
 *     removed from the cache asynchronously via {@link java.util.concurrent.ExecutorService}.
 */
@ThreadSafe
public final class ChannelCache implements StatusDataProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ChannelCache.class);
  private static final String DIRECTPATH = "Directpath";
  private static final String CLOUDPATH = "Cloudpath";
  private final LoadingCache<WindmillServiceAddress, ManagedChannel> channelCache;

  @GuardedBy("this")
  private UserWorkerGrpcFlowControlSettings currentFlowControlSettings =
      UserWorkerGrpcFlowControlSettings.getDefaultInstance();

  private ChannelCache(
      WindmillChannelFactory channelFactory,
      RemovalListener<WindmillServiceAddress, ManagedChannel> onChannelRemoved,
      Executor channelCloser) {
    this.channelCache =
        CacheBuilder.newBuilder()
            .removalListener(RemovalListeners.asynchronous(onChannelRemoved, channelCloser))
            .build(
                new CacheLoader<WindmillServiceAddress, ManagedChannel>() {
                  @Override
                  public ManagedChannel load(WindmillServiceAddress key) {
                    return channelFactory.create(resolveFlowControlSettings(key.getKind()), key);
                  }

                  private UserWorkerGrpcFlowControlSettings resolveFlowControlSettings(
                      WindmillServiceAddress.Kind addressType) {
                    synchronized (ChannelCache.this) {
                      if (currentFlowControlSettings.equals(
                          UserWorkerGrpcFlowControlSettings.getDefaultInstance())) {
                        return addressType == AUTHENTICATED_GCP_SERVICE_ADDRESS
                            ? WindmillChannels.DEFAULT_DIRECTPATH_FLOW_CONTROL_SETTINGS
                            : WindmillChannels.DEFAULT_CLOUDPATH_FLOW_CONTROL_SETTINGS;
                      }

                      return currentFlowControlSettings;
                    }
                  }
                });
  }

  public static ChannelCache create(WindmillChannelFactory channelFactory) {
    return new ChannelCache(
        channelFactory,
        // Shutdown the channels as they get removed from the cache, so they do not leak.
        notification -> shutdownChannel(checkNotNull(notification.getValue())),
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("GrpcChannelCloser").build()));
  }

  @VisibleForTesting
  public static ChannelCache forTesting(
      WindmillChannelFactory channelFactory, Runnable onChannelShutdown) {
    return new ChannelCache(
        channelFactory,
        // Shutdown the channels as they get removed from the cache, so they do not leak.
        // Add hook for testing so that we don't have to sleep/wait for arbitrary time in test.
        notification -> {
          shutdownChannel(checkNotNull(notification.getValue()));
          onChannelShutdown.run();
        },
        // Run the removal synchronously on the calling thread to prevent waiting on asynchronous
        // tasks to run and make unit tests deterministic. In testing, we verify that things are
        // removed from the cache.
        MoreExecutors.directExecutor());
  }

  private static void shutdownChannel(ManagedChannel channel) {
    channel.shutdown();
    try {
      channel.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Couldn't gracefully close gRPC channel={}", channel, e);
    }
    channel.shutdownNow();
  }

  public ManagedChannel get(WindmillServiceAddress windmillServiceAddress) {
    return channelCache.getUnchecked(windmillServiceAddress);
  }

  public synchronized void consumeFlowControlSettings(
      UserWorkerGrpcFlowControlSettings flowControlSettings) {
    if (!flowControlSettings.equals(currentFlowControlSettings)) {
      // Refreshing the cache will asynchronously terminate the old channels via the removalListener
      // and return a newly created one on the next Cache.load(address). This could be expensive so
      // only do it when we have received new flow control settings.
      LOG.debug("Updating flow control settings {}.", flowControlSettings);
      currentFlowControlSettings = flowControlSettings;
      channelCache.asMap().keySet().forEach(channelCache::refresh);
    }
  }

  public void remove(WindmillServiceAddress windmillServiceAddress) {
    channelCache.invalidate(windmillServiceAddress);
  }

  public void clear() {
    channelCache.invalidateAll();
    channelCache.cleanUp();
  }

  /**
   * Checks to see if the cache is empty. May block the calling thread to perform any pending
   * removal/insert operations first before checking the size. Should be only used for testing.
   */
  @VisibleForTesting
  boolean isEmpty() {
    channelCache.cleanUp();
    return channelCache.size() == 0;
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    synchronized (this) {
      if (currentFlowControlSettings == null) {
        writer.format(
            "Directpath gRPC flow control settings: [%s]",
            WindmillChannels.DEFAULT_DIRECTPATH_FLOW_CONTROL_SETTINGS);
        writer.format(
            "Cloudpath gRPC flow control settings: [%s]",
            WindmillChannels.DEFAULT_CLOUDPATH_FLOW_CONTROL_SETTINGS);
      } else {
        writer.format("gRPC flow control settings: [%s]", currentFlowControlSettings);
      }
    }
    writer.write("Active gRPC Channels:<br>");
    channelCache
        .asMap()
        .forEach(
            (address, channel) -> {
              writer.format(
                  "Address: [%s]; Channel: [%s]; AddressType:[%s].",
                  address,
                  channel,
                  address.getKind() == AUTHENTICATED_GCP_SERVICE_ADDRESS ? DIRECTPATH : CLOUDPATH);
              writer.write("<br>");
            });
  }
}
