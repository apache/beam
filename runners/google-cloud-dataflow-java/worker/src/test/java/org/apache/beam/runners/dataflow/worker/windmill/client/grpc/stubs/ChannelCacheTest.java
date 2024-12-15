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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
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
    return ChannelCache.forTesting(channelFactory, () -> {});
  }

  @After
  public void cleanUp() {
    if (cache != null) {
      cache.clear();
    }
  }

  private ManagedChannel newChannel(String channelName) {
    return WindmillChannelFactory.inProcessChannel(channelName);
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
            ignored -> newChannel(channelName), notifyWhenChannelClosed::countDown);

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
            ignored -> newChannel(channelName), notifyWhenChannelClosed::countDown);
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    ManagedChannel cachedChannel = cache.get(someAddress);
    cache.clear();
    notifyWhenChannelClosed.await();
    assertTrue(cache.isEmpty());
    assertTrue(cachedChannel.isShutdown());
  }
}
