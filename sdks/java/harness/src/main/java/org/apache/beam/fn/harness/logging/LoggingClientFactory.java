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
package org.apache.beam.fn.harness.logging;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;

/**
 * A factory for {@link LoggingClient}s. Provides {@link BeamFnLoggingClient} if the logging service
 * is enabled, otherwise provides a no-op client.
 */
public class LoggingClientFactory {

  private LoggingClientFactory() {}

  /**
   * A factory for {@link LoggingClient}s. Provides {@link BeamFnLoggingClient} if the logging
   * service is enabled, otherwise provides a no-op client.
   */
  public static LoggingClient createAndStart(
      PipelineOptions options,
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory) {
    if (options.as(SdkHarnessOptions.class).getEnableLogViaFnApi()) {
      return BeamFnLoggingClient.createAndStart(options, apiServiceDescriptor, channelFactory);
    } else {
      return new NoOpLoggingClient();
    }
  }

  static final class NoOpLoggingClient implements LoggingClient {
    @Override
    public CompletableFuture<?> terminationFuture() {
      return CompletableFuture.completedFuture(new Object());
    }

    @Override
    public void close() throws Exception {}
  }
}
