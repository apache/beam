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
package org.apache.beam.runners.dataflow.worker;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Indicates that an output element was too large. */
public class OutputTooLargeException extends RuntimeException {
  public OutputTooLargeException(String reason) {
    super(
        reason
            + " See https://cloud.google.com/dataflow/docs/guides/common-errors#key-commit-too-large-exception.");
  }

  /** Returns whether an exception was caused by a {@link OutputTooLargeException}. */
  public static boolean isCausedByOutputTooLargeException(@Nullable Throwable t) {
    while (t != null) {
      if (t instanceof OutputTooLargeException) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }
}
