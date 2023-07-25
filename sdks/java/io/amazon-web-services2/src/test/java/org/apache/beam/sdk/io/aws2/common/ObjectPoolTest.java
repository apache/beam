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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.junit.Rule;
import org.junit.Test;

public class ObjectPoolTest {
  Function<String, AutoCloseable> provider = spy(new Provider());
  ObjectPool<String, AutoCloseable> pool = new ObjectPool<>(provider, obj -> obj.close());

  @Rule public ExpectedLogs logs = ExpectedLogs.none(ObjectPool.class);

  class ResourceTask implements Callable<AutoCloseable> {
    @Override
    public AutoCloseable call() {
      AutoCloseable client = pool.retain("config");
      pool.retain("config");
      pool.release(client);
      verifyNoInteractions(client);
      pool.release(client);
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

    assertThat(pool.retain(config1)).isSameAs(pool.retain(config1));
    assertThat(pool.retain(config1)).isNotSameAs(pool.retain(config2));
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
      client = pool.retain(config);
    }

    for (int i = 0; i < sharedInstances - 1; i++) {
      pool.release(client);
    }
    verifyNoInteractions(client);
    // verify close on last release
    pool.release(client);
    verify(client).close();
    // verify further attempts to release have no effect
    pool.release(client);
    verifyNoMoreInteractions(client);
  }

  @Test
  public void closeClientsOnceReleasedByKey() throws Exception {
    String config = "config";
    int sharedInstances = 10;

    AutoCloseable client = null;
    for (int i = 0; i < sharedInstances; i++) {
      client = pool.retain(config);
    }

    for (int i = 0; i < sharedInstances - 1; i++) {
      pool.releaseByKey(config);
    }
    verifyNoInteractions(client);
    // verify close on last release
    pool.releaseByKey(config);
    verify(client).close();
    // verify further attempts to release have no effect
    pool.releaseByKey(config);
    verifyNoMoreInteractions(client);
  }

  @Test
  public void recreateClientOnceReleased() throws Exception {
    String config = "config";
    AutoCloseable client1 = pool.retain(config);
    pool.release(client1);
    verify(client1).close();

    AutoCloseable client2 = pool.retain(config);
    verifyNoInteractions(client2);

    verify(provider, times(2)).apply(config);
    assertThat(client1).isNotSameAs(client2);
  }

  @Test
  public void releaseWithError() throws Exception {
    Exception onClose = new Exception("error on close");

    AutoCloseable client = pool.retain("config");
    doThrow(onClose).when(client).close();
    pool.release(client);

    verify(client).close();
    logs.verifyWarn("Exception destroying pooled object.", onClose);
  }

  static class Provider implements Function<String, AutoCloseable> {
    @Override
    public AutoCloseable apply(String configName) {
      return mock(AutoCloseable.class, configName);
    }
  };
}
