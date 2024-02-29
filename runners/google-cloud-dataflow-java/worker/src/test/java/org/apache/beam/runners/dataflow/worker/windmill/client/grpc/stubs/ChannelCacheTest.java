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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ChannelCacheTest {

  private ChannelCache cache;

  private ChannelCache newCache(
      boolean useIsolatedChannels,
      Function<WindmillServiceAddress, ManagedChannel> channelFactory) {
    return new ChannelCache(useIsolatedChannels, channelFactory);
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
  public void testLoadingCacheReturnsExistingChannel_noIsolatedChannels() {
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

    cache = newCache(false, channelFactory);
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    // Initial call to load the cache.
    cache.get(someAddress);

    ManagedChannel cachedChannel = cache.get(someAddress);
    assertSame(channel, cachedChannel);
    verify(channelFactory, times(1)).apply(eq(someAddress));
    assertFalse(cachedChannel instanceof IsolationChannel);
  }

  @Test
  public void testLoadingCacheReturnsExistingChannel_useIsolatedChannels() {
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

    cache = newCache(true, channelFactory);
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    // First get loads the cache.
    ManagedChannel loadedChannel = cache.get(someAddress);
    // Second get should return loaded value.
    ManagedChannel cachedChannel = cache.get(someAddress);
    assertSame(loadedChannel, cachedChannel);
    verify(channelFactory, times(1)).apply(eq(someAddress));
  }

  @Test
  public void testLoadingCacheReturnsLoadsChannelWhenNotPresent_noIsolationChannel() {
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

    cache = newCache(false, channelFactory);
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    ManagedChannel cachedChannel = cache.get(someAddress);
    assertSame(channel, cachedChannel);
    verify(channelFactory, times(1)).apply(eq(someAddress));
    assertFalse(cachedChannel instanceof IsolationChannel);
  }

  @Test
  public void testLoadingCacheReturnsLoadsChannelWhenNotPresent_useIsolationChannel() {
    String channelName = "existingChannel";
    Function<WindmillServiceAddress, ManagedChannel> channelFactory =
        spy(
            new Function<WindmillServiceAddress, ManagedChannel>() {
              @Override
              public ManagedChannel apply(WindmillServiceAddress windmillServiceAddress) {
                return newChannel(channelName);
              }
            });

    cache = newCache(true, channelFactory);
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    ManagedChannel cachedChannel = cache.get(someAddress);
    verify(channelFactory, times(1)).apply(eq(someAddress));
    assertTrue(cachedChannel instanceof IsolationChannel);
  }

  @Test
  public void testRemoveAndClose() {
    String channelName = "existingChannel";
    cache = newCache(true, ignored -> newChannel(channelName));
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    ManagedChannel cachedChannel = cache.get(someAddress);
    cache.removeAndClose(someAddress);
    assertTrue(cachedChannel.isShutdown());
    assertTrue(cache.isEmpty());
  }

  @Test
  public void testClear() {
    String channelName = "existingChannel";
    cache = newCache(true, ignored -> newChannel(channelName));
    WindmillServiceAddress someAddress = mock(WindmillServiceAddress.class);
    ManagedChannel cachedChannel = cache.get(someAddress);
    cache.clear();
    assertTrue(cachedChannel.isShutdown());
    assertTrue(cache.isEmpty());
  }
}
