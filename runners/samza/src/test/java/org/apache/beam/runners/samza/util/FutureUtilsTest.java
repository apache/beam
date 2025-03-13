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
package org.apache.beam.runners.samza.util;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@linkplain FutureUtils}. */
public final class FutureUtilsTest {
  private static final List<String> RESULTS = ImmutableList.of("hello", "world");

  @Test
  public void testFlattenFuturesForCollection() {
    CompletionStage<Collection<String>> resultFuture =
        FutureUtils.flattenFutures(
            ImmutableList.of(
                CompletableFuture.completedFuture("hello"),
                CompletableFuture.completedFuture("world")));

    CompletionStage<Void> validationFuture =
        resultFuture.thenAccept(
            actualResults -> {
              Assert.assertEquals(
                  "Expected flattened results to contain {hello, world}", RESULTS, actualResults);
            });

    validationFuture.toCompletableFuture().join();
  }

  @Test
  public void testFlattenFuturesForFailedFuture() {
    CompletionStage<Collection<String>> resultFuture =
        FutureUtils.flattenFutures(
            ImmutableList.of(
                CompletableFuture.completedFuture("hello"),
                createFailedFuture(new RuntimeException())));

    CompletionStage<Void> validationFuture =
        resultFuture.handle(
            (results, ex) -> {
              Assert.assertTrue(
                  "Expected exception to be of RuntimeException", ex instanceof RuntimeException);
              return null;
            });

    validationFuture.toCompletableFuture().join();
  }

  @Test
  public void testWaitForAllFutures() {
    CountDownLatch latch = new CountDownLatch(1);
    CompletionStage<Collection<String>> resultFuture =
        FutureUtils.flattenFutures(
            ImmutableList.of(
                CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        latch.await();
                      } catch (InterruptedException e) {
                        return "";
                      }

                      return "hello";
                    }),
                CompletableFuture.supplyAsync(
                    () -> {
                      latch.countDown();
                      return "world";
                    })));

    CompletionStage<Void> validationFuture =
        resultFuture.thenAccept(
            actualResults -> {
              Assert.assertEquals(
                  "Expected flattened results to contain {hello, world}", RESULTS, actualResults);
            });

    validationFuture.toCompletableFuture().join();
  }

  private static CompletionStage<String> createFailedFuture(Throwable t) {
    CompletableFuture<String> future = new CompletableFuture<>();
    future.completeExceptionally(t);
    return future;
  }
}
