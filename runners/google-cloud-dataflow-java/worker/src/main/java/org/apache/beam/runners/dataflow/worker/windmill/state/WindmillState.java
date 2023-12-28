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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;

/**
 * Abstract base class for all Windmill state.
 *
 * <p>Note that these are not thread safe; each state object is associated with a key and thus only
 * accessed by a single thread at once.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@NotThreadSafe
abstract class WindmillState {
  protected Supplier<Closeable> scopedReadStateSupplier;
  protected WindmillStateReader reader;

  /**
   * Return an asynchronously computed {@link Windmill.WorkItemCommitRequest}. The request should be
   * of a form that can be merged with others (only add to repeated fields).
   */
  abstract Future<Windmill.WorkItemCommitRequest> persist(WindmillStateCache.ForKeyAndFamily cache)
      throws IOException;

  /** Prepare this (possibly reused from cache) state for reading from {@code reader} if needed. */
  void initializeForWorkItem(
      WindmillStateReader reader, Supplier<Closeable> scopedReadStateSupplier) {
    this.reader = reader;
    this.scopedReadStateSupplier = scopedReadStateSupplier;
  }

  /**
   * This (now cached) state should never need to interact with the reader until the next work item.
   * Clear it to prevent space leaks. The reader will be reset by {@link #initializeForWorkItem}
   * upon the next work item.
   */
  void cleanupAfterWorkItem() {
    this.reader = null;
    this.scopedReadStateSupplier = null;
  }

  Closeable scopedReadState() {
    return scopedReadStateSupplier.get();
  }
}
