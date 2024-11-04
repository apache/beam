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
package org.apache.beam.sdk.io.solace.write;

import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.data.Solace.PublishResult;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This will receive all the publishing results asynchronously, from the callbacks done by Solace
 * when the ack of publishing a persistent message is received. This is then used by the finish
 * bundle method of the writer to emit the corresponding results as the output of the write
 * connector.
 */
@Internal
public final class PublishResultsReceiver {
  private final ConcurrentLinkedQueue<PublishResult> resultsQueue = new ConcurrentLinkedQueue<>();

  public @Nullable PublishResult pollResults() {
    return resultsQueue.poll();
  }

  public boolean addResult(PublishResult result) {
    return resultsQueue.add(result);
  }
}
