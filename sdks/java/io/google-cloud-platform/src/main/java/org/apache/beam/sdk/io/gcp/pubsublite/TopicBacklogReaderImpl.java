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
package org.apache.beam.sdk.io.gcp.pubsublite;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.TopicStatsClient;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;

final class TopicBacklogReaderImpl implements TopicBacklogReader {
  private final TopicStatsClient client;
  private final TopicPath topicPath;

  public TopicBacklogReaderImpl(TopicStatsClient client, TopicPath topicPath) {
    this.client = client;
    this.topicPath = topicPath;
  }

  private static Optional<Timestamp> minTimestamp(Optional<Timestamp> t1, Timestamp t2) {
    if (!t1.isPresent() || Timestamps.compare(t1.get(), t2) > 0) {
      return Optional.of(t2);
    }
    return t1;
  }

  @Override
  public ApiFuture<ComputeMessageStatsResponse> computeMessageStats(
      Map<Partition, Offset> subscriptionState) {
    List<ApiFuture<ComputeMessageStatsResponse>> perPartitionFutures =
        subscriptionState.entrySet().stream()
            .map(
                e ->
                    client.computeMessageStats(
                        topicPath, e.getKey(), e.getValue(), Offset.of(Integer.MAX_VALUE)))
            .collect(Collectors.toList());
    return ApiFutures.transform(
        ApiFutures.allAsList(perPartitionFutures),
        responses -> {
          Optional<Timestamp> minPublishTime = Optional.empty();
          Optional<Timestamp> minEventTime = Optional.empty();
          long messageBytes = 0;
          long messageCount = 0;
          for (ComputeMessageStatsResponse response : responses) {
            messageBytes += response.getMessageBytes();
            messageCount += response.getMessageCount();
            if (response.hasMinimumPublishTime()) {
              minPublishTime = minTimestamp(minPublishTime, response.getMinimumPublishTime());
            }
            if (response.hasMinimumEventTime()) {
              minEventTime = minTimestamp(minPublishTime, response.getMinimumEventTime());
            }
          }
          ComputeMessageStatsResponse.Builder builder =
              ComputeMessageStatsResponse.newBuilder()
                  .setMessageBytes(messageBytes)
                  .setMessageCount(messageCount);
          minPublishTime.ifPresent(builder::setMinimumPublishTime);
          minEventTime.ifPresent(builder::setMinimumEventTime);
          return builder.build();
        },
        MoreExecutors.directExecutor());
  }
}
