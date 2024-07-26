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

import com.google.cloud.RetryHelper;
import com.solacesystems.jcsmp.JCSMPException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All the writer threads share the same instance of this class, to control for the number of
 * clients that are connected to Solace, and minimize problems with quotas and limits.
 *
 * <p>This class maintains a map of all the session open in a worker, and control the size of that
 * map, to avoid creating more sessions than Solace could handle.
 *
 * <p>This class is a singleton, and should be initialized only once. Calling the factory method
 * {@link #instance(int, SessionServiceFactory)} will initialize the singleton if it has not been
 * initialized before. That method is idempotent.
 *
 * <p>All the methods in this class are thread-safe.
 */
final class SolaceWriteSessionsHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SolaceWriteSessionsHandler.class);

  private static final AtomicReference<Optional<SolaceWriteSessionsHandler>> instance =
      new AtomicReference<>(Optional.empty());
  private static final Object createProducersLock = new Object();

  private static final AtomicBoolean producersCreated = new AtomicBoolean(false);

  private static final ConcurrentHashMap<Integer, SessionService> sessionsMap =
      new ConcurrentHashMap<>(DEFAULT_WRITER_CLIENTS_PER_WORKER);

  private final int producersMapCardinality;

  private final SessionServiceFactory sessionServiceFactory;

  private SolaceWriteSessionsHandler(
      int producersMapCardinality, SessionServiceFactory sessionServiceFactory)
      throws UnknownHostException {
    this.producersMapCardinality = producersMapCardinality;
    this.sessionServiceFactory = sessionServiceFactory;
    createAllSessions();
  }

  // Factory method for singleton
  public static SolaceWriteSessionsHandler instance(
      int producersMapCardinality, SessionServiceFactory sessionServiceFactory)
      throws UnknownHostException {
    instance.compareAndSet(
        Optional.empty(),
        Optional.of(
            new SolaceWriteSessionsHandler(producersMapCardinality, sessionServiceFactory)));
    assert instance.get().isPresent();
    return instance.get().get();
  }

  public SessionService getSessionService(int producerIndex)
      throws UnknownHostException, JCSMPException {
    if (sessionsMap == null) {
      LOG.warn(
          "SolaceIO.Write: Producers map is null in getBundlePublisher, recreating" + " producers");
      resetAndRecreate();
    }

    assert sessionsMap != null;

    SessionService sessionService = sessionsMap.get(producerIndex);
    if (sessionService == null) {
      throw new IllegalStateException(
          String.format("SolaceIO.Write: No session with index %d", producerIndex));
    }
    return sessionService;
  }

  private void createAllSessions() throws UnknownHostException {
    synchronized (createProducersLock) {
      // Return from the blocking call immediately if it was already executed
      if (producersCreated.get()) {
        return;
      }

      long threadId = Thread.currentThread().getId();
      String workerName = InetAddress.getLocalHost().getHostName();

      long start = System.nanoTime();
      for (int producerIndex = 0; producerIndex < producersMapCardinality; producerIndex++) {
        try {
          SessionService s = sessionServiceFactory.create();
          sessionsMap.put(producerIndex, s);
        } catch (RetryHelper.RetryHelperException e) {
          LOG.error(
              "SolaceIO.Write: worker={} thread={}, clients limit"
                  + " reached, only {} producers created",
              workerName,
              threadId,
              producerIndex);
          break;
        }
      }

      if (sessionsMap.isEmpty()) {
        String msg =
            String.format(
                "SolaceIO.Write: Clients limit reached and no producers created"
                    + " for worker %s. You should reduce the number of workers"
                    + " used by Solace, the number of clients per worker and/or"
                    + " the number of workers of the job. Or increase the"
                    + " limits in Solace.",
                workerName);
        LOG.error(msg);
        throw new RuntimeException(msg);
      }

      long end = System.nanoTime();
      long latency = TimeUnit.NANOSECONDS.toMillis(end - start);

      LOG.info(
          "SolaceIO.Write: {} producers created in {} ms, worker={}" + " thread={}",
          producersMapCardinality,
          latency,
          workerName,
          threadId);

      producersCreated.set(true);
    }
  }

  /**
   * The Solace session sometimes lose track with Solace, specially under very heavy workloads.
   *
   * <p>This will release any resource that is currently acquired by this state manager and will
   * reconnect again.
   *
   * <p>This is a slow operation, so this should only be called if it's essential to recover a
   * session.
   */
  public void resetAndRecreate() throws JCSMPException, UnknownHostException {
    producersCreated.set(false);
    disconnectFromSolace();
    createAllSessions();
  }

  /**
   * Disconnect all the sessions from Solace, and clear the state.
   *
   * <p>Use this method before reconnecting again.
   */
  public void disconnectFromSolace() {
    synchronized (createProducersLock) {
      // Return from the blocking call immediately if it was already executed
      if (sessionsMap == null) {
        return;
      }

      sessionsMap.values().forEach(SessionService::close);
      sessionsMap.clear();
    }
  }
}
