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

/**
 * Indicates that the work is no longer valid and should be canceled. It is thrown as a signal for
 * upper layers to mark the work as failed. This is different from WorkItemCancelledException, which
 * is thrown after marking the work as failed.
 */
public class WorkCancellingException extends RuntimeException {

  public WorkCancellingException(long sharding_key) {
    super("Work cancelling exception for key " + sharding_key);
  }

  public WorkCancellingException(Throwable cause) {
    super(cause);
  }

  /** Returns whether an exception was caused by a {@link WorkCancellingException}. */
  public static boolean isWorkCancellingException(Throwable t) {
    @Nullable Throwable throwable = t;
    while (throwable != null) {
      if (throwable instanceof WorkCancellingException) {
        return true;
      }
      throwable = throwable.getCause();
    }
    return false;
  }
}
