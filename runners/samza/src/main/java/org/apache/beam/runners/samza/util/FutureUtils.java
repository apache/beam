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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A util class to handle java 8 {@link CompletableFuture} and {@link CompletionStage}. */
@SuppressWarnings({"rawtypes"})
public final class FutureUtils {
  /**
   * Flattens the input future collection and returns a single future comprising the results of all
   * the futures.
   *
   * @param inputFutures input future collection
   * @param <T> result type of the input future
   * @return a single {@link CompletionStage} that contains the results of all the input futures.
   */
  public static <T> CompletionStage<Collection<T>> flattenFutures(
      Collection<CompletionStage<T>> inputFutures) {
    CompletableFuture<T>[] futures = inputFutures.toArray(new CompletableFuture[0]);

    return CompletableFuture.allOf(futures)
        .thenApply(
            ignored -> {
              final List<T> result =
                  Stream.of(futures).map(CompletableFuture::join).collect(Collectors.toList());
              return result;
            });
  }

  public static <T> CompletionStage<Collection<T>> combineFutures(
      CompletionStage<Collection<T>> future1, CompletionStage<Collection<T>> future2) {

    if (future1 == null) {
      return future2;
    } else if (future2 == null) {
      return future1;
    } else {
      return future1.thenCombine(
          future2,
          (c1, c2) -> {
            c1.addAll(c2);
            return c1;
          });
    }
  }
}
