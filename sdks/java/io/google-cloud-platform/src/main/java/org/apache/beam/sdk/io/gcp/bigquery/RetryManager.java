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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import java.time.Instant;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.Operation.Context;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Queues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;

/**
 * Retry manager used by Storage API operations. This class manages a sequence of operations (e.g.
 * sequential appends to a stream) and retries of those operations. If any one operation fails, then
 * all subsequent operations are expected to fail true and will all be retried.
 */
class RetryManager<ResultT, ContextT extends Context<ResultT>> {
  private Queue<Operation<ResultT, ContextT>> operations;
  private final BackOff backoff;
  private static final ExecutorService executor =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder().setNameFormat("BeamBQRetryManager-%d").build());

  // Enum returned by onError indicating whether errors should be retried.
  enum RetryType {
    // The in-flight operations will not be retried.
    DONT_RETRY,
    // All operations will be retried.
    RETRY_ALL_OPERATIONS
  };

  static class WrappedFailure extends Throwable {
    @Nullable private final Object result;

    public WrappedFailure(@Nullable Object result) {
      this.result = result;
    }

    @Nullable
    Object getResult() {
      return result;
    }
  }

  RetryManager(Duration initialBackoff, Duration maxBackoff, int maxRetries) {
    this.operations = Queues.newArrayDeque();
    backoff =
        FluentBackoff.DEFAULT
            .withInitialBackoff(initialBackoff)
            .withMaxBackoff(maxBackoff)
            .withMaxRetries(maxRetries)
            .backoff();
  }

  RetryManager(
      Duration initialBackoff, Duration maxBackoff, int maxRetries, Counter throttledTimeCounter) {
    this.operations = Queues.newArrayDeque();
    backoff =
        FluentBackoff.DEFAULT
            .withInitialBackoff(initialBackoff)
            .withMaxBackoff(maxBackoff)
            .withMaxRetries(maxRetries)
            .withThrottledTimeCounter(throttledTimeCounter)
            .backoff();
  }

  static class Operation<ResultT, ContextT extends Context<ResultT>> {
    static class Context<ResultT> {
      private @Nullable Throwable error = null;
      private @Nullable ResultT result = null;
      private @Nullable Instant operationStartTime = null;
      private @Nullable Instant operationEndTime = null;

      public void setError(@Nullable Throwable error) {
        this.error = error;
      }

      public @Nullable Throwable getError() {
        return error;
      }

      public void setResult(@Nullable ResultT result) {
        this.result = result;
      }

      public @Nullable ResultT getResult() {
        return result;
      }

      public void setOperationStartTime(@Nullable Instant operationStartTime) {
        this.operationStartTime = operationStartTime;
      }

      public @Nullable Instant getOperationStartTime() {
        return operationStartTime;
      }

      public void setOperationEndTime(@Nullable Instant operationEndTime) {
        this.operationEndTime = operationEndTime;
      }

      public @Nullable Instant getOperationEndTime() {
        return operationEndTime;
      }
    }

    private final Function<ContextT, ApiFuture<ResultT>> runOperation;
    private final Function<Iterable<ContextT>, RetryType> onError;
    private final Consumer<ContextT> onSuccess;
    private final Function<ResultT, Boolean> hasSucceeded;
    @Nullable private ApiFuture<ResultT> future = null;
    @Nullable private Callback<ResultT> callback = null;
    ContextT context;

    public Operation(
        Function<ContextT, ApiFuture<ResultT>> runOperation,
        Function<Iterable<ContextT>, RetryType> onError,
        Consumer<ContextT> onSuccess,
        Function<ResultT, Boolean> hasSucceeded,
        ContextT context) {
      this.runOperation = runOperation;
      this.onError = onError;
      this.onSuccess = onSuccess;
      this.hasSucceeded = hasSucceeded;
      this.context = context;
    }

    void run(Executor executor) {
      this.context.setOperationStartTime(Instant.now());
      this.context.setOperationEndTime(null);
      this.future = runOperation.apply(context);
      this.callback = new Callback<>(hasSucceeded);
      ApiFutures.addCallback(future, callback, executor);
    }

    boolean await() throws Exception {
      Preconditions.checkStateNotNull(callback);
      callback.await();
      return callback.getFailed();
    }
  }

  private static class Callback<ResultT> implements ApiFutureCallback<ResultT> {
    private final CountDownLatch waiter;
    private final Function<ResultT, Boolean> hasSucceeded;
    @Nullable private Throwable failure = null;
    boolean failed = false;
    @Nullable Instant operationEndTime = null;

    Callback(Function<ResultT, Boolean> hasSucceeded) {
      this.waiter = new CountDownLatch(1);
      this.hasSucceeded = hasSucceeded;
    }

    void await() throws InterruptedException {
      waiter.await();
    }

    boolean await(long timeoutSec) throws InterruptedException {
      return waiter.await(timeoutSec, TimeUnit.SECONDS);
    }

    @Override
    public void onFailure(Throwable t) {
      synchronized (this) {
        operationEndTime = Instant.now();
        failure = t;
        failed = true;
      }
      waiter.countDown();
    }

    @Override
    public void onSuccess(ResultT result) {
      synchronized (this) {
        operationEndTime = Instant.now();
        if (hasSucceeded.apply(result)) {
          failure = null;
        } else {
          failure = new WrappedFailure(result);
          failed = true;
        }
      }
      waiter.countDown();
    }

    @Nullable
    Throwable getFailure() {
      synchronized (this) {
        return failure;
      }
    }

    boolean getFailed() {
      synchronized (this) {
        return failed;
      }
    }

    @Nullable
    Instant getOperationEndTime() {
      synchronized (this) {
        return operationEndTime;
      }
    }
  }

  void addOperation(
      Function<ContextT, ApiFuture<ResultT>> runOperation,
      Function<Iterable<ContextT>, RetryType> onError,
      Consumer<ContextT> onSuccess,
      ContextT context)
      throws Exception {
    addOperation(runOperation, onError, onSuccess, r -> true, context);
  }

  void addOperation(
      Function<ContextT, ApiFuture<ResultT>> runOperation,
      Function<Iterable<ContextT>, RetryType> onError,
      Consumer<ContextT> onSuccess,
      Function<ResultT, Boolean> hasSucceeded,
      ContextT context)
      throws Exception {
    addOperation(new Operation<>(runOperation, onError, onSuccess, hasSucceeded, context));
  }

  void addAndRunOperation(
      Function<ContextT, ApiFuture<ResultT>> runOperation,
      Function<Iterable<ContextT>, RetryType> onError,
      Consumer<ContextT> onSuccess,
      ContextT context)
      throws Exception {
    addAndRunOperation(new Operation<>(runOperation, onError, onSuccess, r -> true, context));
  }

  void addAndRunOperation(
      Function<ContextT, ApiFuture<ResultT>> runOperation,
      Function<Iterable<ContextT>, RetryType> onError,
      Consumer<ContextT> onSuccess,
      Function<ResultT, Boolean> hasSucceeded,
      ContextT context)
      throws Exception {
    addAndRunOperation(new Operation<>(runOperation, onError, onSuccess, hasSucceeded, context));
  }

  void addOperation(Operation<ResultT, ContextT> operation) {
    operations.add(operation);
  }

  void addAndRunOperation(Operation<ResultT, ContextT> operation) {
    operation.run(executor);
    operations.add(operation);
  }

  void run(boolean await) throws Exception {
    for (Operation<ResultT, ContextT> operation : operations) {
      operation.run(executor);
    }
    if (await) {
      await();
    }
  }

  void await() throws Exception {
    while (!this.operations.isEmpty()) {
      Operation<ResultT, ContextT> operation = this.operations.element();
      boolean failed = operation.await();
      @Nullable Callback<?> callback = operation.callback;
      if (callback != null) {
        operation.context.setOperationEndTime(callback.getOperationEndTime());
      }
      if (failed) {
        Throwable failure = Preconditions.checkStateNotNull(operation.callback).getFailure();
        operation.context.setError(failure);
        RetryType retryType =
            operation.onError.apply(
                operations.stream().map(o -> o.context).collect(Collectors.toList()));
        if (retryType == RetryType.DONT_RETRY) {
          operations.clear();
        } else {
          checkState(RetryType.RETRY_ALL_OPERATIONS == retryType);
          if (!BackOffUtils.next(Sleeper.DEFAULT, backoff)) {
            throw new RuntimeException(failure);
          }
          for (Operation<ResultT, ?> awaitOperation : operations) {
            awaitOperation.await();
          }
          // Run all the operations again.
          run(false);
        }
      } else {
        operation.context.setResult(Preconditions.checkStateNotNull(operation.future).get());
        operation.onSuccess.accept(operation.context);
        operations.remove();
      }
    }
  }
}
