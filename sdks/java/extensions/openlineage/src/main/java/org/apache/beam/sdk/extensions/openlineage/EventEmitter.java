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
 * classes of the Spark and Flink integrations: emission is wrapped in the configured circuit
 * breaker and never propagates an exception to the pipeline.
 */
class EventEmitter {

  private static final Logger LOG = LoggerFactory.getLogger(EventEmitter.class);

  private final OpenLineageClient client;
  private final @Nullable CircuitBreaker circuitBreaker;

  EventEmitter(BeamOpenLineageConfig config) {
    TransportConfig transportConfig =
        config.getTransportConfig() == null ? new ConsoleConfig() : config.getTransportConfig();
    FacetsConfig facetsConfig =
        config.getFacetsConfig() == null ? new FacetsConfig() : config.getFacetsConfig();
    this.client =
        OpenLineageClient.builder()
            .transport(new TransportFactory(transportConfig).build())
            .disableFacets(facetsConfig.getEffectiveDisabledFacets())
            .build();
    this.circuitBreaker =
        config.getCircuitBreaker() == null
            ? null
            : new CircuitBreakerFactory(config.getCircuitBreaker()).build();
  }

  /** Emits the event; failures are logged and swallowed so lineage never breaks the job. */
  void emit(OpenLineage.RunEvent event) {
    try {
      if (circuitBreaker != null) {
        circuitBreaker.run(
            () -> {
              client.emit(event);
              return Boolean.TRUE;
            });
      } else {
        client.emit(event);
      }
      LOG.debug("Emitted OpenLineage event: {} run {}", event.getEventType(), event.getRun());
    } catch (RuntimeException e) {
      LOG.warn("Failed to emit OpenLineage event, swallowing exception", e);
    }
  }
}
