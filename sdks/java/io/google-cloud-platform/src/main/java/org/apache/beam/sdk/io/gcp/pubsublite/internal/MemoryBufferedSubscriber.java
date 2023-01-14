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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import com.google.api.core.ApiService;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.util.Optional;

interface MemoryBufferedSubscriber extends ApiService {

  /**
   * Get the current fetch offset of this subscriber. This offset will be less than or equal to all
   * future messages returned by this object.
   */
  Offset fetchOffset();

  /**
   * Notify this subscriber that all messages that have been removed with `pop` should no longer be
   * counted against the memory budget.
   *
   * <p>Acquire a new memory buffer and allow any bytes which are now available to be sent to this
   * job.
   */
  void rebuffer() throws ApiException;

  /** Return the head message from the buffer if it exists, or empty() otherwise. */
  Optional<SequencedMessage> peek();

  /** Remove the head message from the buffer. Throws if no messages exist in the buffer. */
  void pop();
}
