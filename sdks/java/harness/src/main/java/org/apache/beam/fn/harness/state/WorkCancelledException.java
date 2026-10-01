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
package org.apache.beam.fn.harness.state;

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Indicates that the work item is no longer valid on the runner and should be cancelled without
 * logging an error.
 */
public class WorkCancelledException extends RuntimeException {

  public WorkCancelledException(String message) {
    super(message);
  }

  public WorkCancelledException(Throwable cause) {
    super(cause);
  }

  /** Returns whether an exception was caused by a {@link WorkCancelledException}. */
  public static boolean isWorkCancelledException(@Nullable Throwable t) {
    return t != null
        && !Iterables.isEmpty(
            Iterables.filter(Throwables.getCausalChain(t), WorkCancelledException.class));
  }
}
