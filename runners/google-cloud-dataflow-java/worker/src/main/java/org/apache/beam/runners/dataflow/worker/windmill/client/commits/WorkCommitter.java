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
package org.apache.beam.runners.dataflow.worker.windmill.client.commits;

import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.annotations.Internal;

/**
 * Commits {@link org.apache.beam.runners.dataflow.worker.streaming.Work} that has completed user
 * processing back to persistence layer.
 */
@Internal
@ThreadSafe
public interface WorkCommitter {

  /** Starts internal processing of commits. */
  void start();

  /**
   * Add a commit to {@link WorkCommitter}. This may be block the calling thread depending on
   * underlying implementations, and persisting to the persistence layer may be done asynchronously.
   */
  void commit(Commit commit);

  /** Number of bytes currently trying to be committed to the backing persistence layer. */
  long currentActiveCommitBytes();

  /**
   * Stops internal processing of commits. In progress and subsequent commits may be canceled or
   * dropped.
   */
  void stop();

  /**
   * Number of internal workers {@link WorkCommitter} uses to commit work to the backing persistence
   * layer.
   */
  int parallelism();
}
