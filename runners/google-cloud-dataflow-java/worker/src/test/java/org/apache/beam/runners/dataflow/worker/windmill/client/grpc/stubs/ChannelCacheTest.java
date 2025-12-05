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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.UserWorkerGrpcFlowControlSettings;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ChannelCacheTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  private ChannelCache cache;

  private static ChannelCache newCache(
      Function<WindmillServiceAddress, ManagedChannel> channelFactory) {
    return ChannelCache.forTesting((ignored, address) -> channelFactory.apply(address), () -> {});
  }

  @After
  public void cleanUp() {
    if (cache != null) {
      cache.clear();
    }
  }

  private ManagedChannel newChannel(String channelName) {
    return WindmillChannels.inProcessChannel(channelName);
  }

  @Test
  public void testLoadingCacheReturnsExistingChannel() {
    String channelName = "existingChannel";
    ManagedChannel channel = newChannel(channelName);
    Function<WindmillServiceAddress, ManagedChannel> channelFactory =
        spy(
            new Function<WindmillServiceAddress, ManagedChannel>() {
              @Override
              public ManagedChannel apply(WindmillServiceAddress windmillServiceAddress) {
                return channel;
              }
            });

    cache = newCache(channelFactory);
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    // Initial call to load the cache.
    assertThat(cache.get(someAddress)).isEqualTo(channel);

    ManagedChannel cachedChannel = cache.get(someAddress);
    assertSame(channel, cachedChannel);
    verify(channelFactory, times(1)).apply(eq(someAddress));
  }

  @Test
  public void testLoadingCacheReturnsLoadsChannelWhenNotPresent() {
    String channelName = "existingChannel";
    ManagedChannel channel = newChannel(channelName);
    Function<WindmillServiceAddress, ManagedChannel> channelFactory =
        spy(
            new Function<WindmillServiceAddress, ManagedChannel>() {
              @Override
              public ManagedChannel apply(WindmillServiceAddress windmillServiceAddress) {
                return channel;
              }
            });

    cache = newCache(channelFactory);
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    ManagedChannel cachedChannel = cache.get(someAddress);
    assertSame(channel, cachedChannel);
    verify(channelFactory, times(1)).apply(eq(someAddress));
  }

  @Test
  public void testRemoveAndClose() throws InterruptedException {
    String channelName = "existingChannel";
    CountDownLatch notifyWhenChannelClosed = new CountDownLatch(1);
    cache =
        ChannelCache.forTesting(
            (ignoredFlowControlSettings, ignoredServiceAddress) -> newChannel(channelName),
            notifyWhenChannelClosed::countDown);

    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    ManagedChannel cachedChannel = cache.get(someAddress);
    cache.remove(someAddress);
    // Assert that the removal happened before we check to see if the shutdowns happen to confirm
    // that removals are async.
    assertTrue(cache.isEmpty());

    // Assert that the channel gets shutdown.
    notifyWhenChannelClosed.await();
    assertTrue(cachedChannel.isShutdown());

    // Get should return a new channel, since we removed the last one.
    ManagedChannel newChannel = cache.get(someAddress);
    assertThat(newChannel).isNotSameInstanceAs(cachedChannel);
  }

  @Test
  public void testClear() throws InterruptedException {
    String channelName = "existingChannel";
    CountDownLatch notifyWhenChannelClosed = new CountDownLatch(1);
    cache =
        ChannelCache.forTesting(
            (ignoredFlowControlSettings, ignoredServiceAddress) -> newChannel(channelName),
            notifyWhenChannelClosed::countDown);
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    ManagedChannel cachedChannel = cache.get(someAddress);
    cache.clear();
    notifyWhenChannelClosed.await();
    assertTrue(cache.isEmpty());
    assertTrue(cachedChannel.isShutdown());
  }

  @Test
  public void testConsumeFlowControlSettings() throws InterruptedException {
    String channelName = "channel";
    CountDownLatch notifyWhenChannelClosed = new CountDownLatch(1);
    AtomicInteger newChannelsCreated = new AtomicInteger();
    AtomicReference<UserWorkerGrpcFlowControlSettings> consumedFlowControlSettings =
        new AtomicReference<>();
    cache =
        ChannelCache.forTesting(
            (newFlowControlSettings, ignoredServiceAddress) -> {
              ManagedChannel channel = newChannel(channelName);
              newChannelsCreated.incrementAndGet();
              consumedFlowControlSettings.set(newFlowControlSettings);
              return channel;
            },
            notifyWhenChannelClosed::countDown);
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    when(someAddress.getKind())
        .thenReturn(WindmillServiceAddress.Kind.AUTHENTICATED_GCP_SERVICE_ADDRESS);

    // Load the cache w/ this first get.
    ManagedChannel cachedChannel = cache.get(someAddress);

    // Load flow control settings.
    UserWorkerGrpcFlowControlSettings flowControlSettings =
        UserWorkerGrpcFlowControlSettings.newBuilder()
            .setEnableAutoFlowControl(true)
            .setOnReadyThresholdBytes(1)
            .setFlowControlWindowBytes(1)
            .build();
    cache.consumeFlowControlSettings(flowControlSettings);

    // This get should reload the cache, since flow control settings have changed.
    ManagedChannel reloadedChannel = cache.get(someAddress);
    notifyWhenChannelClosed.await();
    assertThat(cachedChannel).isNotSameInstanceAs(reloadedChannel);
    assertTrue(cachedChannel.isShutdown());
    assertFalse(reloadedChannel.isShutdown());
    assertThat(newChannelsCreated.get()).isEqualTo(2);
    assertThat(cache.get(someAddress)).isSameInstanceAs(reloadedChannel);
    assertThat(consumedFlowControlSettings.get()).isEqualTo(flowControlSettings);
  }

  @Test
  public void testConsumeFlowControlSettings_UsesDefaultOverridesForDirect()
      throws InterruptedException {
    String channelName = "channel";
    AtomicReference<CountDownLatch> notifyWhenChannelClosed =
        new AtomicReference<>(new CountDownLatch(1));
    AtomicInteger newChannelsCreated = new AtomicInteger();
    AtomicReference<UserWorkerGrpcFlowControlSettings> consumedFlowControlSettings =
        new AtomicReference<>();
    cache =
        ChannelCache.forTesting(
            (newFlowControlSettings, ignoredServiceAddress) -> {
              ManagedChannel channel = newChannel(channelName);
              newChannelsCreated.incrementAndGet();
              consumedFlowControlSettings.set(newFlowControlSettings);
              return channel;
            },
            () -> notifyWhenChannelClosed.get().countDown());
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    when(someAddress.getKind())
        .thenReturn(WindmillServiceAddress.Kind.AUTHENTICATED_GCP_SERVICE_ADDRESS);

    UserWorkerGrpcFlowControlSettings emptyFlowControlSettings =
        UserWorkerGrpcFlowControlSettings.newBuilder().build();

    // Load the cache w/ this first get.
    ManagedChannel cachedChannel = cache.get(someAddress);
    // Verify that the appropriate default was used.
    assertThat(consumedFlowControlSettings.get())
        .isEqualTo(WindmillChannels.DEFAULT_DIRECTPATH_FLOW_CONTROL_SETTINGS);

    // Load empty flow control settings.
    cache.consumeFlowControlSettings(emptyFlowControlSettings);
    // This get shouldn't reload the cache, since the same default flow control settings
    // should be used.
    assertThat(consumedFlowControlSettings.get())
        .isEqualTo(WindmillChannels.DEFAULT_DIRECTPATH_FLOW_CONTROL_SETTINGS);
    assertThat(cachedChannel).isSameInstanceAs(cache.get(someAddress));

    // This get should reload the cache, since flow control settings have changed
    UserWorkerGrpcFlowControlSettings flowControlSettingsModified =
        UserWorkerGrpcFlowControlSettings.newBuilder().setEnableAutoFlowControl(true).build();
    cache.consumeFlowControlSettings(flowControlSettingsModified);
    ManagedChannel reloadedChannel = cache.get(someAddress);
    notifyWhenChannelClosed.get().await();
    assertThat(cachedChannel).isNotSameInstanceAs(reloadedChannel);
    assertTrue(cachedChannel.isShutdown());
    assertFalse(reloadedChannel.isShutdown());
    assertThat(newChannelsCreated.get()).isEqualTo(2);
    assertThat(cache.get(someAddress)).isSameInstanceAs(reloadedChannel);
    assertThat(consumedFlowControlSettings.get()).isEqualTo(flowControlSettingsModified);

    // Change back to empty settings and verify the default is used again.
    notifyWhenChannelClosed.set(new CountDownLatch(1));
    cache.consumeFlowControlSettings(emptyFlowControlSettings);
    ManagedChannel reloadedChannel2 = cache.get(someAddress);
    notifyWhenChannelClosed.get().await();
    assertThat(reloadedChannel2).isNotSameInstanceAs(reloadedChannel);
    assertThat(reloadedChannel2).isNotSameInstanceAs(cachedChannel);
    assertTrue(reloadedChannel.isShutdown());
    assertFalse(reloadedChannel2.isShutdown());
    assertThat(newChannelsCreated.get()).isEqualTo(3);
    assertThat(cache.get(someAddress)).isSameInstanceAs(reloadedChannel2);
    assertThat(consumedFlowControlSettings.get())
        .isEqualTo(WindmillChannels.DEFAULT_DIRECTPATH_FLOW_CONTROL_SETTINGS);
  }

  @Test
  public void testConsumeFlowControlSettings_UsesDefaultOverridesForCloudPath()
      throws InterruptedException {
    String channelName = "channel";
    AtomicReference<CountDownLatch> notifyWhenChannelClosed =
        new AtomicReference<>(new CountDownLatch(1));
    AtomicInteger newChannelsCreated = new AtomicInteger();
    AtomicReference<UserWorkerGrpcFlowControlSettings> consumedFlowControlSettings =
        new AtomicReference<>();
    cache =
        ChannelCache.forTesting(
            (newFlowControlSettings, ignoredServiceAddress) -> {
              ManagedChannel channel = newChannel(channelName);
              newChannelsCreated.incrementAndGet();
              consumedFlowControlSettings.set(newFlowControlSettings);
              return channel;
            },
            () -> notifyWhenChannelClosed.get().countDown());
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    when(someAddress.getKind()).thenReturn(WindmillServiceAddress.Kind.GCP_SERVICE_ADDRESS);

    UserWorkerGrpcFlowControlSettings emptyFlowControlSettings =
        UserWorkerGrpcFlowControlSettings.newBuilder().build();

    // Load the cache w/ this first get.
    ManagedChannel cachedChannel = cache.get(someAddress);
    // Verify that the appropriate default was used.
    assertThat(consumedFlowControlSettings.get())
        .isEqualTo(WindmillChannels.DEFAULT_CLOUDPATH_FLOW_CONTROL_SETTINGS);

    // Load empty flow control settings.
    cache.consumeFlowControlSettings(emptyFlowControlSettings);
    // This get shouldn't reload the cache, since the same default flow control settings
    // should be used.
    assertThat(consumedFlowControlSettings.get())
        .isEqualTo(WindmillChannels.DEFAULT_CLOUDPATH_FLOW_CONTROL_SETTINGS);
    assertThat(cachedChannel).isSameInstanceAs(cache.get(someAddress));

    // This get should reload the cache, since flow control settings have changed
    UserWorkerGrpcFlowControlSettings flowControlSettingsModified =
        UserWorkerGrpcFlowControlSettings.newBuilder().setEnableAutoFlowControl(true).build();
    cache.consumeFlowControlSettings(flowControlSettingsModified);
    ManagedChannel reloadedChannel = cache.get(someAddress);
    notifyWhenChannelClosed.get().await();
    assertThat(cachedChannel).isNotSameInstanceAs(reloadedChannel);
    assertTrue(cachedChannel.isShutdown());
    assertFalse(reloadedChannel.isShutdown());
    assertThat(newChannelsCreated.get()).isEqualTo(2);
    assertThat(cache.get(someAddress)).isSameInstanceAs(reloadedChannel);
    assertThat(consumedFlowControlSettings.get()).isEqualTo(flowControlSettingsModified);

    // Change back to empty settings and verify the default is used again.
    notifyWhenChannelClosed.set(new CountDownLatch(1));
    cache.consumeFlowControlSettings(emptyFlowControlSettings);
    ManagedChannel reloadedChannel2 = cache.get(someAddress);
    notifyWhenChannelClosed.get().await();
    assertThat(reloadedChannel2).isNotSameInstanceAs(reloadedChannel);
    assertThat(reloadedChannel2).isNotSameInstanceAs(cachedChannel);
    assertTrue(reloadedChannel.isShutdown());
    assertFalse(reloadedChannel2.isShutdown());
    assertThat(newChannelsCreated.get()).isEqualTo(3);
    assertThat(cache.get(someAddress)).isSameInstanceAs(reloadedChannel2);
    assertThat(consumedFlowControlSettings.get())
        .isEqualTo(WindmillChannels.DEFAULT_CLOUDPATH_FLOW_CONTROL_SETTINGS);
  }

  @Test
  public void testConsumeFlowControlSettings_sameFlowControlSettings() {
    String channelName = "channel";
    AtomicInteger newChannelsCreated = new AtomicInteger();
    UserWorkerGrpcFlowControlSettings flowControlSettings =
        UserWorkerGrpcFlowControlSettings.newBuilder()
            .setEnableAutoFlowControl(true)
            .setOnReadyThresholdBytes(1)
            .setFlowControlWindowBytes(1)
            .build();
    Queue<UserWorkerGrpcFlowControlSettings> consumedFlowControlSettings =
        new LinkedBlockingQueue<>();
    cache =
        ChannelCache.forTesting(
            (newFlowControlSettings, ignoredServiceAddress) -> {
              ManagedChannel channel = newChannel(channelName);
              newChannelsCreated.incrementAndGet();
              consumedFlowControlSettings.add(newFlowControlSettings);
              return channel;
            },
            () -> {});
    // Load flow control settings.
    cache.consumeFlowControlSettings(flowControlSettings);
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    when(someAddress.getKind())
        .thenReturn(WindmillServiceAddress.Kind.AUTHENTICATED_GCP_SERVICE_ADDRESS);
    // Load the cache w/ this first get.
    ManagedChannel cachedChannel = cache.get(someAddress);

    // Load the same flow control settings.
    cache.consumeFlowControlSettings(flowControlSettings);
    // Because the flow control settings are the same, cache should not reload the value for the
    // same address.
    ManagedChannel reloadedChannel = cache.get(someAddress);
    assertThat(cachedChannel).isSameInstanceAs(reloadedChannel);
    assertFalse(cachedChannel.isShutdown());
    assertThat(newChannelsCreated.get()).isEqualTo(1);
    assertThat(consumedFlowControlSettings.size()).isEqualTo(1);
    assertThat(Iterables.getOnlyElement(consumedFlowControlSettings))
        .isEqualTo(flowControlSettings);
  }
}
