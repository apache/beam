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
package org.apache.beam.runners.prism;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import org.apache.beam.fn.harness.ExternalWorkerService;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ExternalWorkerService} {@link GrpcFnServer} encapsulation that {@link #stop}s when
 * {@link StateListener#onStateChanged} is invoked with a {@link PipelineResult.State} that is
 * {@link PipelineResult.State#isTerminal}.
 */
class WorkerService implements StateListener {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerService.class);

  private final ExternalWorkerService worker;
  private @MonotonicNonNull GrpcFnServer<ExternalWorkerService> server;

  WorkerService(PortablePipelineOptions options) {
    this.worker = new ExternalWorkerService(options);
  }

  /** Start the {@link ExternalWorkerService}. */
  void start() throws Exception {
    if (server != null && !server.getServer().isShutdown()) {
      return;
    }

    server = worker.start();
    LOG.info("Starting worker service at {}", getApiServiceDescriptorUrl());
  }

  /**
   * Queries whether the {@link ExternalWorkerService} {@link GrpcFnServer}'s {@link Server} is
   * running.
   */
  boolean isRunning() {
    if (server == null) {
      return false;
    }
    return !server.getServer().isShutdown();
  }

  /**
   * Queries the {@link Endpoints.ApiServiceDescriptor#getUrl} of the {@link ExternalWorkerService}
   * {@link GrpcFnServer}'s {@link Server}. Throws an exception if the {@link WorkerService} has not
   * {@link WorkerService#start}ed.
   */
  String getApiServiceDescriptorUrl() {
    return checkStateNotNull(server, "worker service not started")
        .getApiServiceDescriptor()
        .getUrl();
  }

  /**
   * Updates {@link PortablePipelineOptions#getDefaultEnvironmentConfig} with {@link
   * #getApiServiceDescriptorUrl}. Throws an exception if the {@link WorkerService} has not {@link
   * WorkerService#start}ed.
   */
  PortablePipelineOptions updateDefaultEnvironmentConfig(PortablePipelineOptions options) {
    options.setDefaultEnvironmentConfig(getApiServiceDescriptorUrl());
    return options;
  }

  /**
   * Overrides {@link StateListener#onStateChanged}, invoking {@link #stop} when {@link
   * PipelineResult.State#isTerminal}.
   */
  @Override
  public void onStateChanged(PipelineResult.State state) {
    if (state.isTerminal()) {
      stop();
    }
  }

  /**
   * Stops the {@link ExternalWorkerService} {@link GrpcFnServer}'s {@link Server}. If not {@link
   * WorkerService#isRunning()}, then calling stop is a noop.
   */
  void stop() {
    if (server == null || server.getServer().isShutdown()) {
      return;
    }
    LOG.info("Stopping worker service at {}", getApiServiceDescriptorUrl());
    try {
      server.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
