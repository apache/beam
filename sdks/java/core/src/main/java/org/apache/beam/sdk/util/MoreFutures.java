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
package org.apache.beam.sdk.util;

import com.google.auto.value.AutoValue;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utilities to do future programming with Java 8.
 *
 * <p>Standards for these utilities:
 *
 * <ul>
 *   <li>Always allow thrown exceptions, and they should cause futures to complete exceptionally.
 *   <li>Always return {@link CompletionStage} as a future value.
 *   <li>Return {@link CompletableFuture} only to the <i>producer</i> of a future value.
 * </ul>
 */
public class MoreFutures {

  /**
   * Gets the result of the given future.
   *
   * <p>This utility is provided so consumers of futures need not even convert to {@link
   * CompletableFuture}, an interface that is only suitable for producers of futures.
   */
  public static <T> T get(CompletionStage<T> future)
      throws InterruptedException, ExecutionException {
    return future.toCompletableFuture().get();
  }

  /**
   * Gets the result of the given future.
   *
   * <p>This utility is provided so consumers of futures need not even convert to {@link
   * CompletableFuture}, an interface that is only suitable for producers of futures.
   */
  public static <T> T get(CompletionStage<T> future, long duration, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return future.toCompletableFuture().get(duration, unit);
  }

  /**
   * Indicates whether the future is done.
   *
   * <p>This utility is provided so consumers of futures need not even convert to {@link
   * CompletableFuture}, an interface that is only suitable for producers of futures.
   */
  public static boolean isDone(CompletionStage<?> future) {
    return future.toCompletableFuture().isDone();
  }

  /**
   * Indicates whether the future is cancelled.
   *
   * <p>This utility is provided so consumers of futures need not even convert to {@link
   * CompletableFuture}, an interface that is only suitable for producers of futures.
   */
  public static boolean isCancelled(CompletionStage<?> future) {
    return future.toCompletableFuture().isCancelled();
  }

  /**
   * Like {@link CompletableFuture#supplyAsync(Supplier)} but for {@link ThrowingSupplier}.
   *
   * <p>If the {@link ThrowingSupplier} throws an exception, the future completes exceptionally.
   */
  public static <T> CompletionStage<T> supplyAsync(
      ThrowingSupplier<T> supplier, ExecutorService executorService) {
    CompletableFuture<T> result = new CompletableFuture<>();

    CompletionStage<Void> wrapper =
        CompletableFuture.runAsync(
            () -> {
              try {
                result.complete(supplier.get());
              } catch (InterruptedException e) {
                result.completeExceptionally(e);
                Thread.currentThread().interrupt();
              } catch (Throwable t) {
                result.completeExceptionally(t);
              }
            },
            executorService);
    return wrapper.thenCompose(nothing -> result);
  }

  /**
   * Shorthand for {@link #supplyAsync(ThrowingSupplier, ExecutorService)} using {@link
   * ForkJoinPool#commonPool()}.
   */
  public static <T> CompletionStage<T> supplyAsync(ThrowingSupplier<T> supplier) {
    return supplyAsync(supplier, ForkJoinPool.commonPool());
  }

  /**
   * Like {@link CompletableFuture#runAsync} but for {@link ThrowingRunnable}.
   *
   * <p>If the {@link ThrowingRunnable} throws an exception, the future completes exceptionally.
   */
  public static CompletionStage<Void> runAsync(
      ThrowingRunnable runnable, ExecutorService executorService) {
    CompletableFuture<Void> result = new CompletableFuture<>();

    CompletionStage<Void> wrapper =
        CompletableFuture.runAsync(
            () -> {
              try {
                runnable.run();
                result.complete(null);
              } catch (InterruptedException e) {
                result.completeExceptionally(e);
                Thread.currentThread().interrupt();
              } catch (Throwable t) {
                result.completeExceptionally(t);
              }
            },
            executorService);
    return wrapper.thenCompose(nothing -> result);
  }

  /**
   * Shorthand for {@link #runAsync(ThrowingRunnable, ExecutorService)} using {@link
   * ForkJoinPool#commonPool()}.
   */
  public static CompletionStage<Void> runAsync(ThrowingRunnable runnable) {
    return runAsync(runnable, ForkJoinPool.commonPool());
  }

  /** Like {@link CompletableFuture#allOf} but returning the result of constituent futures. */
  public static <T> CompletionStage<List<T>> allAsList(
      Collection<? extends CompletionStage<? extends T>> futures) {

    // CompletableFuture.allOf completes exceptionally if any of the futures do.
    // We have to gather the results separately.
    CompletionStage<Void> blockAndDiscard =
        CompletableFuture.allOf(futuresToCompletableFutures(futures));

    return blockAndDiscard.thenApply(
        nothing ->
            futures.stream()
                .map(future -> future.toCompletableFuture().join())
                .collect(Collectors.toList()));
  }

  /**
   * An object that represents either a result or an exceptional termination.
   *
   * <p>This is used, for example, in aggregating the results of many future values in {@link
   * #allAsList(Collection)}.
   */
  @SuppressWarnings(
      value = "NM_CLASS_NOT_EXCEPTION",
      justification = "The class does hold an exception; its name is accurate.")
  @AutoValue
  public abstract static class ExceptionOrResult<T> {

    /** Describes whether the result was an exception. */
    public enum IsException {
      EXCEPTION,
      RESULT
    }

    public abstract IsException isException();

    public abstract @Nullable T getResult();

    public abstract @Nullable Throwable getException();

    public static <T> ExceptionOrResult<T> exception(Throwable throwable) {
      return new AutoValue_MoreFutures_ExceptionOrResult(IsException.EXCEPTION, null, throwable);
    }

    public static <T> ExceptionOrResult<T> result(T result) {
      return new AutoValue_MoreFutures_ExceptionOrResult(IsException.EXCEPTION, result, null);
    }
  }

  /** Like {@link #allAsList} but return a list . */
  public static <T> CompletionStage<List<ExceptionOrResult<T>>> allAsListWithExceptions(
      Collection<? extends CompletionStage<? extends T>> futures) {

    // CompletableFuture.allOf completes exceptionally if any of the futures do.
    // We have to gather the results separately.
    CompletionStage<Void> blockAndDiscard =
        CompletableFuture.allOf(futuresToCompletableFutures(futures))
            .whenComplete((ignoredValues, arbitraryException) -> {});

    return blockAndDiscard.thenApply(
        nothing ->
            futures.stream()
                .map(
                    future -> {
                      // The limited scope of the exceptions wrapped allows CancellationException
                      // to still be thrown.
                      try {
                        return ExceptionOrResult.<T>result(future.toCompletableFuture().join());
                      } catch (CompletionException exc) {
                        return ExceptionOrResult.<T>exception(exc);
                      }
                    })
                .collect(Collectors.toList()));
  }

  /**
   * Helper to convert a list of futures into an array for use in {@link CompletableFuture} vararg
   * combinators.
   */
  private static <T> CompletableFuture<? extends T>[] futuresToCompletableFutures(
      Collection<? extends CompletionStage<? extends T>> futures) {
    CompletableFuture<? extends T>[] completableFutures = new CompletableFuture[futures.size()];
    int i = 0;
    for (CompletionStage<? extends T> future : futures) {
      completableFutures[i] = future.toCompletableFuture();
      ++i;
    }
    return completableFutures;
  }
}
