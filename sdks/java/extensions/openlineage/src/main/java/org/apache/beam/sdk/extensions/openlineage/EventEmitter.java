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
package org.apache.beam.sdk.extensions.openlineage;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
import io.openlineage.client.circuitBreaker.CircuitBreakerFactory;
import io.openlineage.client.transports.ConsoleConfig;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportConfig;
import io.openlineage.client.transports.TransportFactory;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends OpenLineage events through the configured transport, mirroring the {@code EventEmitter}
 * classes of the Spark and Flink integrations: construction and emission never propagate an
 * exception to the pipeline — an invalid transport configuration degrades to a non-emitting client
 * with an error logged, and emission is wrapped in the configured circuit breaker.
 */
class EventEmitter {

  private static final Logger LOG = LoggerFactory.getLogger(EventEmitter.class);

  private final @Nullable OpenLineageClient client;
  private final @Nullable CircuitBreaker circuitBreaker;

  EventEmitter(BeamOpenLineageConfig config) {
    this.client = buildClient(config);
    this.circuitBreaker = buildCircuitBreaker(config);
  }

  private static @Nullable OpenLineageClient buildClient(BeamOpenLineageConfig config) {
    try {
      TransportConfig transportConfig =
          config.getTransportConfig() == null ? new ConsoleConfig() : config.getTransportConfig();
      FacetsConfig facetsConfig =
          config.getFacetsConfig() == null ? new FacetsConfig() : config.getFacetsConfig();
      return OpenLineageClient.builder()
          .transport(new TransportFactory(transportConfig).build())
          .disableFacets(facetsConfig.getEffectiveDisabledFacets())
          .build();
    } catch (RuntimeException e) {
      LOG.error(
          "Invalid OpenLineage transport configuration; lineage events will NOT be emitted", e);
      return null;
    }
  }

  private static @Nullable CircuitBreaker buildCircuitBreaker(BeamOpenLineageConfig config) {
    try {
      return config.getCircuitBreaker() == null
          ? null
          : new CircuitBreakerFactory(config.getCircuitBreaker()).build();
    } catch (RuntimeException e) {
      LOG.warn("Invalid OpenLineage circuit breaker configuration; continuing without one", e);
      return null;
    }
  }

  /** Emits the event; failures are logged and swallowed so lineage never breaks the job. */
  void emit(OpenLineage.RunEvent event) {
    final OpenLineageClient localClient = client;
    if (localClient == null) {
      return;
    }
    try {
      if (circuitBreaker != null) {
        circuitBreaker.run(
            () -> {
              localClient.emit(event);
              return Boolean.TRUE;
            });
      } else {
        localClient.emit(event);
      }
      LOG.debug("Emitted OpenLineage event: {} run {}", event.getEventType(), event.getRun());
    } catch (RuntimeException e) {
      LOG.warn("Failed to emit OpenLineage event, swallowing exception", e);
    }
  }

  /** Closes the underlying transport (flushing buffered events); never throws. */
  void close() {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (Exception e) {
      LOG.debug("Failed to close OpenLineage client", e);
    }
  }
}
