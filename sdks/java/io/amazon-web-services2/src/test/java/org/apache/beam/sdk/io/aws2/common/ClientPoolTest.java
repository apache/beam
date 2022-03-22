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

import static java.util.concurrent.ForkJoinPool.commonPool;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClientPoolTest {
  @Spy ClientProvider provider = new ClientProvider();
  ClientPool<Function<String, AutoCloseable>, String, AutoCloseable> pool;

  @Before
  public void init() {
    pool = new ClientPool<>((p, c) -> p.apply(c));
  }

  class ResourceTask implements Callable<AutoCloseable> {
    @Override
    public AutoCloseable call() throws Exception {
      AutoCloseable client = pool.retain(provider, "config");
      pool.retain(provider, "config");
      pool.release(provider, "config");
      verifyNoInteractions(client);
      pool.release(provider, "config");
      return client;
    }
  }

  @Test
  public void concurrentRetainRelease() throws Exception {
    List<ForkJoinTask<AutoCloseable>> futures =
        Stream.generate(() -> new ResourceTask())
            .limit(100000)
            .map(commonPool()::submit)
            .collect(toList());

    futures.stream().forEach(ForkJoinTask::join);
    assertThat(futures.stream().allMatch(ForkJoinTask::isCompletedNormally)).isTrue();

    for (ForkJoinTask<AutoCloseable> future : futures) {
      verify(future.get()).close();
    }
  }

  @Test
  public void shareClientsOfSameConfiguration() {
    String config1 = "config1";
    String config2 = "config2";

    assertThat(pool.retain(provider, config1)).isSameAs(pool.retain(provider, config1));
    assertThat(pool.retain(provider, config1)).isNotSameAs(pool.retain(provider, config2));
    verify(provider, times(2)).apply(anyString());
    verify(provider, times(1)).apply(config1);
    verify(provider, times(1)).apply(config2);
  }

  @Test
  public void closeClientsOnceReleased() throws Exception {
    String config = "config";
    int sharedInstances = 10;

    AutoCloseable client = null;
    for (int i = 0; i < sharedInstances; i++) {
      client = pool.retain(provider, config);
    }

    for (int i = 1; i < sharedInstances; i++) {
      pool.release(provider, config);
    }
    verifyNoInteractions(client);
    // verify close on last release
    pool.release(provider, config);
    verify(client).close();
    // verify further attempts to release have no effect
    pool.release(provider, config);
    verifyNoMoreInteractions(client);
  }

  @Test
  public void recreateClientOnceReleased() throws Exception {
    String config = "config";
    AutoCloseable client1 = pool.retain(provider, config);
    pool.release(provider, config);
    AutoCloseable client2 = pool.retain(provider, config);

    verify(provider, times(2)).apply(config);
    verify(client1).close();
    verifyNoInteractions(client2);
  }

  @Test
  public void releaseWithError() throws Exception {
    String config = "config";
    AutoCloseable client1 = pool.retain(provider, config);
    doThrow(new Exception("error on close")).when(client1).close();
    assertThatThrownBy(() -> pool.release(provider, config)).hasMessage("error on close");

    AutoCloseable client2 = pool.retain(provider, config);
    verify(provider, times(2)).apply(config);
    verify(client1).close();
    verifyNoInteractions(client2);
  }

  static class ClientProvider implements Function<String, AutoCloseable> {
    @Override
    public AutoCloseable apply(String configName) {
      return mock(AutoCloseable.class, configName);
    }
  };
}
