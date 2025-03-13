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

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;

/**
 * The TopicBacklogReader uses the TopicStats API to aggregate the backlog, or the distance between
 * the current cursor and HEAD for a single {subscription, partition} pair.
 */
interface TopicBacklogReader extends AutoCloseable {

  /**
   * Compute and aggregate message statistics for message between the provided start offset and
   * HEAD. This method is blocking.
   *
   * @param offset The current offset of the subscriber.
   * @return A ComputeMessageStatsResponse with the aggregated statistics for messages in the
   *     backlog.
   */
  ComputeMessageStatsResponse computeMessageStats(Offset offset) throws ApiException;
}
