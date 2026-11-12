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
package org.apache.beam.runners.dataflow.worker.util;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.CheckReturnValue;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utility methods for simplifying work with exceptions and throwables. */
@Internal
public final class ExceptionUtils {

  private ExceptionUtils() {}

  /**
   * Returns the {@code throwable} as-is if it is an instance of {@link RuntimeException} throws if
   * it is an {@link Error}, or returns the {@code throwable} wrapped in a {@code RuntimeException}.
   */
  @CheckReturnValue
  public static RuntimeException safeWrapThrowableAsException(Throwable throwable) {
    if (throwable instanceof RuntimeException) {
      return (RuntimeException) throwable;
    } else if (throwable instanceof Error) {
      throw (Error) throwable;
    } else {
      return new RuntimeException(throwable);
    }
  }

  /**
   * Returns whether {@code throwable} or any of its causes is an instance of {@code
   * exceptionClass}.
   */
  public static boolean hasCause(
      @Nullable Throwable throwable, Class<? extends Throwable> exceptionClass) {
    return hasMatchingCause(throwable, exceptionClass::isInstance);
  }

  /**
   * Returns whether {@code throwable} or any of its causes is an instance of either {@code
   * exceptionClass1} or {@code exceptionClass2}.
   */
  public static boolean hasCause(
      @Nullable Throwable throwable,
      Class<? extends Throwable> exceptionClass1,
      Class<? extends Throwable> exceptionClass2) {
    return hasMatchingCause(
        throwable, t -> exceptionClass1.isInstance(t) || exceptionClass2.isInstance(t));
  }

  /** Returns whether {@code throwable} or any of its causes matches the given {@code predicate}. */
  public static boolean hasMatchingCause(
      @Nullable Throwable throwable, Predicate<Throwable> predicate) {
    if (throwable == null) {
      return false;
    }
    if (predicate.test(throwable)) {
      return true;
    }
    Throwable cause = throwable.getCause();
    if (cause == null || cause == throwable) {
      return false;
    }
    if (predicate.test(cause)) {
      return true;
    }
    // Only allocate identity set if we need to traverse further down the chain
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    seen.add(throwable);
    seen.add(cause);
    cause = cause.getCause();
    while (cause != null && seen.add(cause)) {
      if (predicate.test(cause)) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }
}
