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

import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.beam.sdk.io.gcp.pubsublite.PublisherOptions;
import org.apache.beam.sdk.transforms.DoFn;

/** A sink which publishes messages to Pub/Sub Lite. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PubsubLiteSink extends DoFn<PubSubMessage, Void> {
  private final PublisherOptions options;

  @GuardedBy("this")
  private transient RunState runState;

  public PubsubLiteSink(PublisherOptions options) {
    this.options = options;
  }

  private static class RunState {
    private final Deque<ApiFuture<MessageMetadata>> futures = new ArrayDeque<>();

    private final Publisher<MessageMetadata> publisher;

    RunState(PublisherOptions options) {
      publisher =
          PerServerPublisherCache.PUBLISHER_CACHE.get(
              options, () -> new PublisherAssembler(options).newPublisher());
    }

    void publish(PubSubMessage message) {
      futures.add(publisher.publish(message));
    }

    void waitForDone() throws Exception {
      ApiFutures.allAsList(futures).get(1, MINUTES);
    }
  }

  @StartBundle
  public synchronized void startBundle() throws ApiException {
    runState = new RunState(options);
  }

  @ProcessElement
  public synchronized void processElement(@Element PubSubMessage message)
      throws CheckedApiException {
    runState.publish(message);
  }

  // Intentionally don't flush on bundle finish to allow multi-sink client reuse.
  @FinishBundle
  public synchronized void finishBundle() throws Exception {
    runState.waitForDone();
  }
}
