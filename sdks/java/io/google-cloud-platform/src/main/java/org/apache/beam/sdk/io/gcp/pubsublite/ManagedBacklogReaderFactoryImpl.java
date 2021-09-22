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

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class ManagedBacklogReaderFactoryImpl implements ManagedBacklogReaderFactory {
  private final SerializableFunction<SubscriptionPartition, TopicBacklogReader> newReader;

  @GuardedBy("this")
  private final Map<SubscriptionPartition, TopicBacklogReader> readers = new HashMap<>();

  ManagedBacklogReaderFactoryImpl(
      SerializableFunction<SubscriptionPartition, TopicBacklogReader> newReader) {
    this.newReader = newReader;
  }

  private static final class NonCloseableTopicBacklogReader implements TopicBacklogReader {
    private final TopicBacklogReader underlying;

    NonCloseableTopicBacklogReader(TopicBacklogReader underlying) {
      this.underlying = underlying;
    }

    @Override
    public ComputeMessageStatsResponse computeMessageStats(Offset offset) throws ApiException {
      return underlying.computeMessageStats(offset);
    }

    @Override
    public void close() {
      throw new IllegalArgumentException(
          "Cannot call close() on a reader returned from ManagedBacklogReaderFactory.");
    }
  }

  @Override
  public synchronized TopicBacklogReader newReader(SubscriptionPartition subscriptionPartition) {
    return new NonCloseableTopicBacklogReader(
        readers.computeIfAbsent(subscriptionPartition, newReader::apply));
  }

  @Override
  public synchronized void close() {
    readers.values().forEach(TopicBacklogReader::close);
  }
}
