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

import org.apache.beam.runners.dataflow.worker.streaming.WeightedSemaphore;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/** Utility class for commits. */
@Internal
public final class Commits {

  /** Max bytes of commits queued on the user worker. */
  @VisibleForTesting static final int MAX_QUEUED_COMMITS_BYTES = 500 << 20; // 500MB

  private Commits() {}

  public static WeightedSemaphore<Commit> maxCommitByteSemaphore() {
    return WeightedSemaphore.create(MAX_QUEUED_COMMITS_BYTES, Commit::getSize);
  }
}
