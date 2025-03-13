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
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.internal.CursorClient;
import java.util.Map;

class InitialOffsetReaderImpl implements InitialOffsetReader {
  private final CursorClient client;
  private final SubscriptionPath subscription;
  private final Partition partition;

  InitialOffsetReaderImpl(
      CursorClient unownedCursorClient, SubscriptionPath subscription, Partition partition) {
    this.client = unownedCursorClient;
    this.subscription = subscription;
    this.partition = partition;
  }

  @Override
  public Offset read() throws ApiException {
    try {
      Map<Partition, Offset> results = client.listPartitionCursors(subscription).get(1, MINUTES);
      return results.getOrDefault(partition, Offset.of(0));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }
}
