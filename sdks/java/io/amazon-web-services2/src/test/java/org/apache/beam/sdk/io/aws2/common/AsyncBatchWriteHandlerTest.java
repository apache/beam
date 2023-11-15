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

import static java.util.Collections.emptyList;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static java.util.function.Function.identity;
import static org.apache.beam.sdk.io.aws2.common.AsyncBatchWriteHandler.byId;
import static org.apache.beam.sdk.io.aws2.common.AsyncBatchWriteHandler.byPosition;
import static org.apache.beam.sdk.util.FluentBackoff.DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.Duration.millis;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.sdk.io.aws2.common.AsyncBatchWriteHandler.Stats;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.mockito.Mockito;

public class AsyncBatchWriteHandlerTest {
  private static final int CONCURRENCY = 10;

  private CompletableFuture<List<Boolean>> resultsByPos = new CompletableFuture<>();
  private CompletableFuture<List<String>> errorsById = new CompletableFuture<>();

  private AsyncBatchWriteHandler<Integer, Boolean> byPositionHandler(FluentBackoff backoff) {
    SubmitFn<Integer, Boolean> submitFn = Mockito.spy(new SubmitFn<>(() -> resultsByPos));
    Function<Boolean, String> errorFn = success -> success ? null : "REASON";
    return byPosition(CONCURRENCY, backoff, Stats.NONE, submitFn, errorFn);
  }

  private AsyncBatchWriteHandler<String, String> byIdHandler(FluentBackoff backoff) {
    SubmitFn<String, String> submitFn = Mockito.spy(new SubmitFn<>(() -> errorsById));
    Function<String, String> errorFn = err -> "REASON";
    return byId(CONCURRENCY, backoff, Stats.NONE, submitFn, errorFn, identity(), identity());
  }

  @Test
  public void retryOnPartialSuccessByPosition() throws Throwable {
    AsyncBatchWriteHandler<Integer, Boolean> handler =
        byPositionHandler(DEFAULT.withMaxBackoff(millis(1)));
    CompletableFuture<List<Boolean>> pendingResponse1 = new CompletableFuture<>();
    CompletableFuture<List<Boolean>> pendingResponse2 = new CompletableFuture<>();
    CompletableFuture<List<Boolean>> pendingResponse3 = new CompletableFuture<>();

    resultsByPos = pendingResponse1;
    handler.batchWrite("destination", ImmutableList.of(1, 2, 3, 4));

    // 1st attempt
    eventually(5, () -> verify(handler.submitFn, times(1)).apply(anyString(), anyList()));
    verify(handler.submitFn).apply("destination", ImmutableList.of(1, 2, 3, 4));
    assertThat(handler.requestsInProgress()).isEqualTo(1);

    resultsByPos = pendingResponse2;
    pendingResponse1.complete(ImmutableList.of(true, true, false, false));

    // 2nd attempt
    eventually(5, () -> verify(handler.submitFn, times(2)).apply(anyString(), anyList()));
    verify(handler.submitFn).apply("destination", ImmutableList.of(3, 4));
    assertThat(handler.requestsInProgress()).isEqualTo(1);

    // 3rd attempt
    resultsByPos = pendingResponse3;
    pendingResponse2.complete(ImmutableList.of(true, false));

    eventually(5, () -> verify(handler.submitFn, times(3)).apply(anyString(), anyList()));
    verify(handler.submitFn).apply("destination", ImmutableList.of(4));

    assertThat(handler.requestsInProgress()).isEqualTo(1);

    // 4th attempt
    pendingResponse3.complete(ImmutableList.of(true)); // success

    eventually(5, () -> assertThat(handler.requestsInProgress()).isEqualTo(0));
    verify(handler.submitFn, times(3)).apply(anyString(), anyList());
  }

  @Test
  public void retryOnPartialSuccessById() throws Throwable {
    AsyncBatchWriteHandler<String, String> handler = byIdHandler(DEFAULT.withMaxBackoff(millis(1)));
    CompletableFuture<List<String>> pendingResponse1 = new CompletableFuture<>();
    CompletableFuture<List<String>> pendingResponse2 = new CompletableFuture<>();
    CompletableFuture<List<String>> pendingResponse3 = new CompletableFuture<>();

    errorsById = pendingResponse1;
    handler.batchWrite("destination", ImmutableList.of("1", "2", "3", "4"));

    // 1st attempt
    eventually(5, () -> verify(handler.submitFn, times(1)).apply(anyString(), anyList()));
    verify(handler.submitFn).apply("destination", ImmutableList.of("1", "2", "3", "4"));
    assertThat(handler.requestsInProgress()).isEqualTo(1);

    errorsById = pendingResponse2;
    pendingResponse1.complete(ImmutableList.of("3", "4"));

    // 2nd attempt
    eventually(5, () -> verify(handler.submitFn, times(2)).apply(anyString(), anyList()));
    verify(handler.submitFn).apply("destination", ImmutableList.of("3", "4"));
    assertThat(handler.requestsInProgress()).isEqualTo(1);

    // 3rd attempt
    errorsById = pendingResponse3;
    pendingResponse2.complete(ImmutableList.of("4"));

    eventually(5, () -> verify(handler.submitFn, times(3)).apply(anyString(), anyList()));
    verify(handler.submitFn).apply("destination", ImmutableList.of("4"));

    assertThat(handler.requestsInProgress()).isEqualTo(1);

    // 4th attempt
    pendingResponse3.complete(ImmutableList.of()); // success

    eventually(5, () -> assertThat(handler.requestsInProgress()).isEqualTo(0));
    verify(handler.submitFn, times(3)).apply(anyString(), anyList());
  }

  @Test
  public void retryLimitOnPartialSuccessByPosition() throws Throwable {
    AsyncBatchWriteHandler<Integer, Boolean> handler = byPositionHandler(DEFAULT.withMaxRetries(0));

    handler.batchWrite("destination", ImmutableList.of(1, 2, 3, 4));

    resultsByPos.complete(ImmutableList.of(true, true, false, false));

    assertThatThrownBy(() -> handler.waitForCompletion())
        .hasMessageContaining("Exceeded retries")
        .hasMessageEndingWith("REASON for 2 record(s).")
        .isInstanceOf(IOException.class);
    verify(handler.submitFn).apply("destination", ImmutableList.of(1, 2, 3, 4));
  }

  @Test
  public void retryLimitOnPartialSuccessById() throws Throwable {
    AsyncBatchWriteHandler<String, String> handler = byIdHandler(DEFAULT.withMaxRetries(0));

    handler.batchWrite("destination", ImmutableList.of("1", "2", "3", "4"));

    errorsById.complete(ImmutableList.of("3", "4"));

    assertThatThrownBy(() -> handler.waitForCompletion())
        .hasMessageContaining("Exceeded retries")
        .hasMessageEndingWith("REASON for 2 record(s).")
        .isInstanceOf(IOException.class);
    verify(handler.submitFn).apply("destination", ImmutableList.of("1", "2", "3", "4"));
  }

  @Test
  public void propagateErrorOnPutRecords() throws Throwable {
    AsyncBatchWriteHandler<Integer, Boolean> handler = byPositionHandler(DEFAULT);
    handler.batchWrite("destination", emptyList());
    resultsByPos.completeExceptionally(new RuntimeException("Request failed"));

    assertThatThrownBy(() -> handler.batchWrite("destination", emptyList()))
        .hasMessage("Request failed");
    assertThat(handler.hasErrored()).isTrue();
    verify(handler.submitFn).apply("destination", emptyList());
  }

  @Test
  public void propagateErrorWhenPolling() throws Throwable {
    AsyncBatchWriteHandler<Integer, Boolean> handler = byPositionHandler(DEFAULT);
    handler.batchWrite("destination", emptyList());
    handler.checkForAsyncFailure(); // none yet
    resultsByPos.completeExceptionally(new RuntimeException("Request failed"));

    assertThatThrownBy(() -> handler.checkForAsyncFailure()).hasMessage("Request failed");
    assertThat(handler.hasErrored()).isTrue();
    handler.checkForAsyncFailure(); // already reset
  }

  @Test
  public void propagateErrorOnWaitForCompletion() throws Throwable {
    AsyncBatchWriteHandler<Integer, Boolean> handler = byPositionHandler(DEFAULT);
    handler.batchWrite("destination", emptyList());
    resultsByPos.completeExceptionally(new RuntimeException("Request failed"));

    assertThatThrownBy(() -> handler.waitForCompletion()).hasMessage("Request failed");
  }

  @Test
  public void correctlyLimitConcurrency() throws Throwable {
    AsyncBatchWriteHandler<Integer, Boolean> handler = byPositionHandler(DEFAULT);

    // exhaust concurrency limit so that batchWrite blocks
    Runnable task = repeat(CONCURRENCY + 1, () -> handler.batchWrite("destination", emptyList()));
    Future<?> future = commonPool().submit(task);

    eventually(5, () -> assertThat(handler.requestsInProgress()).isEqualTo(CONCURRENCY));
    eventually(
        5, () -> verify(handler.submitFn, times(CONCURRENCY)).apply("destination", emptyList()));
    assertThat(future).isNotDone();

    // complete responses and unblock last request
    resultsByPos.complete(emptyList());

    eventually(
        5,
        () -> verify(handler.submitFn, times(CONCURRENCY + 1)).apply("destination", emptyList()));
    handler.waitForCompletion();
    assertThat(future).isDone();
  }

  static class SubmitFn<T, V> implements BiFunction<String, List<T>, CompletableFuture<List<V>>> {
    private final Supplier<CompletableFuture<List<V>>> resp;

    SubmitFn(Supplier<CompletableFuture<List<V>>> resp) {
      this.resp = resp;
    }

    @Override
    public CompletableFuture<List<V>> apply(String destination, List<T> input) {
      return resp.get();
    }
  }

  private void eventually(int attempts, Runnable fun) {
    for (int i = 0; i < attempts - 1; i++) {
      try {
        Thread.sleep(i * 100);
        fun.run();
        return;
      } catch (AssertionError | InterruptedException t) {
      }
    }
    fun.run();
  }

  private Runnable repeat(int times, ThrowingRunnable fun) {
    return () -> {
      for (int i = 0; i < times; i++) {
        try {
          fun.run();
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
    };
  }
}
