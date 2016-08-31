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

package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link LockedKeyedResourcePool}.
 */
@RunWith(JUnit4.class)
public class LockedKeyedResourcePoolTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private LockedKeyedResourcePool<String, Integer> cache =
      LockedKeyedResourcePool.create();

  @Test
  public void acquireReleaseAcquireLastLoadedOrReleased() throws ExecutionException {
    Optional<Integer> returned = cache.tryAcquire("foo", new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return 3;
      }
    });
    assertThat(returned.get(), equalTo(3));

    cache.release("foo", 4);
    Optional<Integer> reacquired = cache.tryAcquire("foo", new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return 5;
      }
    });
    assertThat(reacquired.get(), equalTo(4));
  }

  @Test
  public void acquireReleaseReleaseThrows() throws ExecutionException {
    Optional<Integer> returned = cache.tryAcquire("foo", new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return 3;
      }
    });
    assertThat(returned.get(), equalTo(3));

    cache.release("foo", 4);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("already a value present");
    thrown.expectMessage("At most one");
    cache.release("foo", 4);
  }

  @Test
  public void releaseBeforeAcquireThrows() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("before a value was acquired");
    cache.release("bar", 3);
  }

  @Test
  public void multipleAcquireWithoutReleaseAbsent() throws ExecutionException {
    Optional<Integer> returned = cache.tryAcquire("foo", new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return 3;
      }
    });
    Optional<Integer> secondReturned = cache.tryAcquire("foo", new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return 3;
      }
    });
    assertThat(secondReturned.isPresent(), is(false));
  }

  @Test
  public void acquireMultipleKeysSucceeds() throws ExecutionException {
    Optional<Integer> returned = cache.tryAcquire("foo", new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return 3;
      }
    });
    Optional<Integer> secondReturned = cache.tryAcquire("bar", new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return 4;
      }
    });

    assertThat(returned.get(), equalTo(3));
    assertThat(secondReturned.get(), equalTo(4));
  }

  @Test
  public void acquireThrowsExceptionWrapped() throws ExecutionException {
    final Exception cause = new Exception("checkedException");
    thrown.expect(ExecutionException.class);
    thrown.expectCause(equalTo(cause));
    cache.tryAcquire("foo", new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        throw cause;
      }
    });
  }

  @Test
  public void acquireThrowsRuntimeExceptionWrapped() throws ExecutionException {
    final RuntimeException cause = new RuntimeException("UncheckedException");
    thrown.expect(UncheckedExecutionException.class);
    thrown.expectCause(equalTo(cause));
    cache.tryAcquire("foo", new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        throw cause;
      }
    });
  }

  @Test
  public void acquireThrowsErrorWrapped() throws ExecutionException {
    final Error cause = new Error("Error");
    thrown.expect(ExecutionError.class);
    thrown.expectCause(equalTo(cause));
    cache.tryAcquire("foo", new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        throw cause;
      }
    });
  }
}
