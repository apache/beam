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
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.TopicStatsClient;
import com.google.cloud.pubsublite.internal.TopicStatsClientSettings;
import java.io.Serializable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Ticker;

@AutoValue
abstract class TopicBacklogReaderSettings implements Serializable {
  private static final long serialVersionUID = -4001752066450248673L;

  /** The topic path for this backlog reader. */
  abstract TopicPath topicPath();

  abstract Partition partition();

  static Builder newBuilder() {
    return new AutoValue_TopicBacklogReaderSettings.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {

    // Required parameters.
    abstract Builder setTopicPath(TopicPath topicPath);

    abstract Builder setPartition(Partition partition);

    abstract TopicBacklogReaderSettings build();
  }

  private TopicStatsClient newClient() {
    return TopicStatsClient.create(
        TopicStatsClientSettings.newBuilder()
            .setRegion(topicPath().location().extractRegion())
            .build());
  }

  TopicBacklogReader instantiate() throws ApiException {
    TopicBacklogReader impl = new TopicBacklogReaderImpl(newClient(), topicPath(), partition());
    return new LimitingTopicBacklogReader(impl, Ticker.systemTicker());
  }
}
