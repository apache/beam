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
package org.apache.beam.sdk.io.solace.write;

import static org.apache.beam.sdk.io.solace.SolaceIO.DEFAULT_WRITER_CLIENTS_PER_WORKER;

import com.google.auto.value.AutoValue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;

/**
 * All the writer threads belonging to the same factory share the same instance of this class, to
 * control for the number of clients that are connected to Solace, and minimize problems with quotas
 * and limits.
 *
 * <p>This class maintains a map of all the session open in a worker, and control the size of that
 * map, to avoid creating more sessions than Solace could handle.
 *
 * <p>This class is thread-safe and creates a pool of producers per SessionServiceFactory. If there
 * is only a Write transform in the pipeline, this is effectively a singleton. If there are more
 * than one, each {@link SessionServiceFactory} instance keeps their own pool of producers.
 */
final class SolaceWriteSessionsHandler {
  private static final ConcurrentHashMap<SessionConfigurationIndex, SessionService> sessionsMap =
      new ConcurrentHashMap<>(DEFAULT_WRITER_CLIENTS_PER_WORKER);

  public static SessionService getSessionService(
      int producerIndex, SessionServiceFactory sessionServiceFactory) {
    SessionConfigurationIndex key =
        SessionConfigurationIndex.builder()
            .producerIndex(producerIndex)
            .sessionServiceFactory(sessionServiceFactory)
            .build();
    return sessionsMap.computeIfAbsent(
        key, SolaceWriteSessionsHandler::createSessionAndStartProducer);
  }

  private static SessionService createSessionAndStartProducer(SessionConfigurationIndex key) {
    SessionServiceFactory factory = key.sessionServiceFactory();
    SessionService sessionService = factory.create();
    // Start the producer now that the initialization is locked for other threads
    sessionService.getProducer();
    return sessionService;
  }

  /** Disconnect all the sessions from Solace, and clear the corresponding state. */
  public static void disconnectFromSolace(SessionServiceFactory factory, int producersCardinality) {
    for (int i = 0; i < producersCardinality; i++) {
      SessionConfigurationIndex key =
          SessionConfigurationIndex.builder()
              .producerIndex(i)
              .sessionServiceFactory(factory)
              .build();

      SessionService sessionService = sessionsMap.remove(key);
      if (sessionService != null) {
        sessionService.close();
      }
    }
  }

  @AutoValue
  abstract static class SessionConfigurationIndex {
    abstract int producerIndex();

    abstract SessionServiceFactory sessionServiceFactory();

    static Builder builder() {
      return new AutoValue_SolaceWriteSessionsHandler_SessionConfigurationIndex.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder producerIndex(int producerIndex);

      abstract Builder sessionServiceFactory(SessionServiceFactory sessionServiceFactory);

      abstract SessionConfigurationIndex build();
    }
  }
}
