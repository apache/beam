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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientCall;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.MethodDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Status;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ManagedChannel} that creates a dynamic # of cached channels to the same endpoint such
 * that each active rpc has its own channel.
 */
@Internal
public class IsolationChannel extends ManagedChannel {
  private static final Logger LOG = LoggerFactory.getLogger(IsolationChannel.class);

  private final Supplier<ManagedChannel> channelFactory;

  @GuardedBy("this")
  private final List<ManagedChannel> channelCache;

  @GuardedBy("this")
  private final Set<ManagedChannel> usedChannels;

  @GuardedBy("this")
  private boolean shutdownStarted = false;

  private final String authority;

  // Expected that supplier returns channels to the same endpoint with the same authority.
  public static IsolationChannel create(Supplier<ManagedChannel> channelFactory) {
    return new IsolationChannel(channelFactory);
  }

  @VisibleForTesting
  IsolationChannel(Supplier<ManagedChannel> channelFactory) {
    this.channelFactory = channelFactory;
    this.channelCache = new ArrayList<>();
    ManagedChannel channel = channelFactory.get();
    this.authority = channel.authority();
    this.channelCache.add(channel);
    this.usedChannels = new HashSet<>();
  }

  @Override
  public String authority() {
    return authority;
  }

  /** Pick an unused channel from the set or create one and then create a {@link ClientCall}. */
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
    ManagedChannel channel = getChannel();
    return new ReleasingClientCall<>(this, channel, channel.newCall(methodDescriptor, callOptions));
  }

  private synchronized ManagedChannel getChannel() {
    if (!channelCache.isEmpty()) {
      ManagedChannel result = channelCache.remove(channelCache.size() - 1);
      usedChannels.add(result);
      return result;
    }
    if (shutdownStarted) {
      // Avoid creating a new channel if we're shutting down and just use an existing one
      // that has already started shutting down.
      Preconditions.checkArgument(!usedChannels.isEmpty());
      return usedChannels.iterator().next();
    }

    ManagedChannel result = channelFactory.get();
    usedChannels.add(result);
    return result;
  }

  @Override
  public ManagedChannel shutdown() {
    ArrayList<ManagedChannel> channels = new ArrayList<>();
    synchronized (this) {
      shutdownStarted = true;
      channels.addAll(usedChannels);
      channels.addAll(channelCache);
    }
    for (ManagedChannel channel : channels) {
      channel.shutdown();
    }
    return this;
  }

  @Override
  public ManagedChannel shutdownNow() {
    ArrayList<ManagedChannel> channels = new ArrayList<>();
    synchronized (this) {
      shutdownStarted = true;
      channels.addAll(usedChannels);
      channels.addAll(channelCache);
    }
    for (ManagedChannel channel : channels) {
      channel.shutdownNow();
    }
    return this;
  }

  @Override
  public synchronized boolean isShutdown() {
    if (!shutdownStarted) {
      return false;
    }
    for (ManagedChannel channel : usedChannels) {
      if (!channel.isShutdown()) return false;
    }
    for (ManagedChannel channel : channelCache) {
      if (!channel.isShutdown()) return false;
    }
    return true;
  }

  @Override
  public synchronized boolean isTerminated() {
    if (!shutdownStarted) {
      return false;
    }
    for (ManagedChannel channel : usedChannels) {
      if (!channel.isTerminated()) return false;
    }
    for (ManagedChannel channel : channelCache) {
      if (!channel.isTerminated()) return false;
    }
    return true;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long endTimeNanos = System.nanoTime() + unit.toNanos(timeout);
    ArrayList<ManagedChannel> channels = new ArrayList<>();
    synchronized (this) {
      if (!shutdownStarted) {
        return false;
      }
      if (isTerminated()) {
        return true;
      }
      channels.addAll(usedChannels);
      channels.addAll(channelCache);
    }
    // Block outside the synchronized section.
    for (ManagedChannel channel : channels) {
      long awaitTimeNanos = endTimeNanos - System.nanoTime();
      if (awaitTimeNanos <= 0) {
        break;
      }
      channel.awaitTermination(awaitTimeNanos, TimeUnit.NANOSECONDS);
    }
    return isTerminated();
  }

  synchronized void releaseChannelAfterCall(ManagedChannel managedChannel) {
    boolean wasInUsedChannels = usedChannels.remove(managedChannel);
    if (wasInUsedChannels) {
      channelCache.add(managedChannel);
    } else {
      // If we've started shutting down, we may have started multiple calls for a channel.
      Preconditions.checkState(
          shutdownStarted && channelCache.contains(managedChannel),
          "Channel released that was not used, and not in shutdown.");
    }
  }

  /** ClientCall wrapper that adds channel back to the cache on completion. */
  private static class ReleasingClientCall<ReqT, RespT>
      extends SimpleForwardingClientCall<ReqT, RespT> {
    private final IsolationChannel isolationChannel;

    private final ManagedChannel channel;
    private final AtomicBoolean wasReleased = new AtomicBoolean();

    public ReleasingClientCall(
        IsolationChannel isolationChannel,
        ManagedChannel channel,
        ClientCall<ReqT, RespT> delegate) {
      super(delegate);
      this.isolationChannel = isolationChannel;
      this.channel = channel;
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      try {
        super.start(
            new SimpleForwardingClientCallListener<RespT>(responseListener) {
              @Override
              public void onClose(Status status, Metadata trailers) {
                try {
                  super.onClose(status, trailers);
                } finally {
                  releaseChannelOnce();
                }
              }
            },
            headers);
      } catch (Exception e) {
        releaseChannelOnce();
        throw e;
      }
    }

    private void releaseChannelOnce() {
      if (!wasReleased.getAndSet(true)) {
        isolationChannel.releaseChannelAfterCall(channel);
      } else {
        LOG.warn(
            "The entry was already released. This indicates that onClose() was called multiple times.");
      }
    }
  }
}
