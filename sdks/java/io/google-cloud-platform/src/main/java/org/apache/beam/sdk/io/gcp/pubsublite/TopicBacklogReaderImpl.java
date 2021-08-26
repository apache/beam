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

import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.TopicStatsClient;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TopicBacklogReaderImpl implements TopicBacklogReader {
  private static final Logger LOG = LoggerFactory.getLogger(TopicBacklogReaderImpl.class);
  private final TopicStatsClient client;
  private final TopicPath topicPath;
  private final Partition partition;

  public TopicBacklogReaderImpl(TopicStatsClient client, TopicPath topicPath, Partition partition) {
    this.client = client;
    this.topicPath = topicPath;
    this.partition = partition;
  }

  @Override
  @SuppressWarnings("assignment.type.incompatible")
  public ComputeMessageStatsResponse computeMessageStats(Offset offset) throws ApiException {
    try {
      return client
          .computeMessageStats(topicPath, partition, offset, Offset.of(Integer.MAX_VALUE))
          .get();
    } catch (ExecutionException e) {
      @Nonnull Throwable cause = checkNotNull(e.getCause());
      throw toCanonical(cause).underlying;
    } catch (InterruptedException e) {
      throw toCanonical(e).underlying;
    }
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (Exception e) {
      LOG.warn("Failed to close topic stats client.", e);
    }
  }
}
