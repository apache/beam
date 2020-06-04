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
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.PublishMetadata;
import com.google.cloud.pubsublite.internal.CloseableMonitor;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.common.util.concurrent.Monitor.Guard;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.grpc.StatusException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.beam.sdk.io.gcp.pubsublite.PublisherOrError.Kind;
import org.apache.beam.sdk.transforms.DoFn;

/** A sink which publishes messages to Pub/Sub Lite. */
class PubsubLiteSink extends DoFn<Message, Void> {
  private final PublisherOptions options;

  private transient CloseableMonitor monitor;

  @GuardedBy("monitor.monitor")
  private transient PublisherOrError publisherOrError;

  @GuardedBy("monitor.monitor")
  private transient int outstanding;

  @GuardedBy("monitor.monitor")
  private transient Deque<StatusException> errorsSinceLastFinish;

  private static final Executor executor = Executors.newCachedThreadPool();

  PubsubLiteSink(PublisherOptions options) {
    this.options = options;
  }

  @Setup
  public void setup() throws StatusException {
    Publisher<PublishMetadata> publisher;
    if (options.usesCache()) {
      publisher = PerServerPublisherCache.PUBLISHER_CACHE.get(options);
    } else {
      publisher = options.getPublisher();
    }
    monitor = new CloseableMonitor();
    try (CloseableMonitor.Hold h = monitor.enter()) {
      outstanding = 0;
      errorsSinceLastFinish = new ArrayDeque<>();
      publisherOrError = PublisherOrError.ofPublisher(publisher);
    }
    publisher.addListener(
        new Listener() {
          @Override
          public void failed(State s, Throwable t) {
            try (CloseableMonitor.Hold h = monitor.enter()) {
              publisherOrError = PublisherOrError.ofError(ExtractStatus.toCanonical(t));
            }
          }
        },
        MoreExecutors.directExecutor());
    if (!options.usesCache()) {
      publisher.startAsync();
    }
  }

  @ProcessElement
  public void processElement(@Element Message message) throws StatusException {
    ApiFuture<PublishMetadata> future;
    try (CloseableMonitor.Hold h = monitor.enter()) {
      ++outstanding;
      if (publisherOrError.getKind() == Kind.ERROR) {
        throw publisherOrError.error();
      }
      future = publisherOrError.publisher().publish(message);
    }
    // Add outside of monitor in case future is completed inline.
    ApiFutures.addCallback(
        future,
        new ApiFutureCallback<PublishMetadata>() {
          @Override
          public void onSuccess(PublishMetadata publishMetadata) {
            try (CloseableMonitor.Hold h = monitor.enter()) {
              --outstanding;
            }
          }

          @Override
          public void onFailure(Throwable t) {
            try (CloseableMonitor.Hold h = monitor.enter()) {
              --outstanding;
              errorsSinceLastFinish.push(ExtractStatus.toCanonical(t));
            }
          }
        },
        executor);
  }

  // Intentionally don't flush on bundle finish to allow multi-sink client reuse.
  @FinishBundle
  public void finishBundle() throws StatusException, IOException {
    try (CloseableMonitor.Hold h =
        monitor.enterWhenUninterruptibly(
            new Guard(monitor.monitor) {
              @Override
              public boolean isSatisfied() {
                return outstanding == 0;
              }
            })) {
      if (!errorsSinceLastFinish.isEmpty()) {
        StatusException canonical = errorsSinceLastFinish.pop();
        while (!errorsSinceLastFinish.isEmpty()) {
          canonical.addSuppressed(errorsSinceLastFinish.pop());
        }
        throw canonical;
      }
      if (publisherOrError.getKind() == Kind.ERROR) {
        throw publisherOrError.error();
      }
    }
  }
}
