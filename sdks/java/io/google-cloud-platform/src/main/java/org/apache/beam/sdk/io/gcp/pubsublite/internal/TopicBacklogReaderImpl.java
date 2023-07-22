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

import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.TopicStatsClient;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;

final class TopicBacklogReaderImpl implements TopicBacklogReader {
  private final TopicStatsClient client;
  private final TopicPath topicPath;
  private final Partition partition;

  public TopicBacklogReaderImpl(TopicStatsClient client, TopicPath topicPath, Partition partition) {
    this.client = client;
    this.topicPath = topicPath;
    this.partition = partition;
  }

  @Override
  @SuppressWarnings("assignment")
  public ComputeMessageStatsResponse computeMessageStats(Offset offset) throws ApiException {
    try {
      return client
          .computeMessageStats(topicPath, partition, offset, Offset.of(Long.MAX_VALUE))
          .get(1, MINUTES);
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  @Override
  public void close() {
    client.close();
  }
}
