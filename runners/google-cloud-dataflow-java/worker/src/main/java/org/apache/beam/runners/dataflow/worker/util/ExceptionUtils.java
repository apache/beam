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

import javax.annotation.CheckReturnValue;
import org.apache.beam.sdk.annotations.Internal;

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
}
