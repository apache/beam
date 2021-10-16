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

import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsublite.PublisherOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/** A map of working publishers by PublisherOptions. */
class PublisherCache implements AutoCloseable {
  @GuardedBy("this")
  private final HashMap<PublisherOptions, Publisher<MessageMetadata>> livePublishers =
      new HashMap<>();

  private synchronized void evict(PublisherOptions options) {
    livePublishers.remove(options);
  }

  synchronized Publisher<MessageMetadata> get(PublisherOptions options) throws ApiException {
    Publisher<MessageMetadata> publisher = livePublishers.get(options);
    if (publisher != null) {
      return publisher;
    }
    publisher = new PublisherAssembler(options).newPublisher();
    livePublishers.put(options, publisher);
    publisher.addListener(
        new Listener() {
          @Override
          public void failed(State s, Throwable t) {
            evict(options);
          }
        },
        SystemExecutors.getFuturesExecutor());
    publisher.startAsync().awaitRunning();
    return publisher;
  }

  @VisibleForTesting
  synchronized void set(PublisherOptions options, Publisher<MessageMetadata> toCache) {
    livePublishers.put(options, toCache);
  }

  @Override
  public synchronized void close() {
    livePublishers.forEach(((options, publisher) -> publisher.stopAsync()));
    livePublishers.clear();
  }
}
