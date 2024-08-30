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
package org.apache.beam.io.requestresponse;

import io.grpc.ChannelCredentials;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest;
import org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse;
import org.apache.beam.testinfra.mockapis.echo.v1.EchoServiceGrpc;
import org.apache.beam.testinfra.mockapis.echo.v1.EchoServiceGrpc.EchoServiceBlockingStub;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

/**
 * Implements {@link Caller} and {@link SetupTeardown} to call the {@link EchoServiceGrpc}. The
 * purpose of {@link EchoGRPCCallerWithSetupTeardown} is support integration tests.
 */
class EchoGRPCCallerWithSetupTeardown implements Caller<EchoRequest, EchoResponse>, SetupTeardown {

  static EchoGRPCCallerWithSetupTeardown of(URI uri) {
    return new EchoGRPCCallerWithSetupTeardown(uri);
  }

  private final URI uri;
  private transient @MonotonicNonNull ManagedChannel cachedManagedChannel;
  private transient @MonotonicNonNull EchoServiceBlockingStub cachedBlockingStub;
  private static final ChannelCredentials DEFAULT_CREDENTIALS = InsecureChannelCredentials.create();

  private EchoGRPCCallerWithSetupTeardown(URI uri) {
    this.uri = uri;
  }

  /**
   * Overrides {@link Caller#call} invoking the {@link EchoServiceGrpc} with a {@link EchoRequest},
   * returning either a successful {@link EchoResponse} or throwing either a {@link
   * UserCodeExecutionException}, a {@link UserCodeTimeoutException}, or a {@link
   * UserCodeQuotaException}.
   */
  @Override
  public EchoResponse call(EchoRequest request) throws UserCodeExecutionException {
    try {
      return cachedBlockingStub.echo(request);
    } catch (StatusRuntimeException e) {
      switch (e.getStatus().getCode()) {
        case RESOURCE_EXHAUSTED:
          throw new UserCodeQuotaException(e);
        case DEADLINE_EXCEEDED:
          throw new UserCodeTimeoutException(e);
        default:
          throw new UserCodeExecutionException(e);
      }
    }
  }

  /**
   * Overrides {@link SetupTeardown#setup} to initialize the {@link ManagedChannel} and {@link
   * EchoServiceBlockingStub}.
   */
  @Override
  public void setup() throws UserCodeExecutionException {
    cachedManagedChannel =
        NettyChannelBuilder.forTarget(uri.toString(), DEFAULT_CREDENTIALS).build();
    cachedBlockingStub = EchoServiceGrpc.newBlockingStub(cachedManagedChannel);
  }

  /** Overrides {@link SetupTeardown#teardown} to shut down the {@link ManagedChannel}. */
  @Override
  public void teardown() throws UserCodeExecutionException {
    if (cachedManagedChannel != null && cachedManagedChannel.isShutdown()) {
      cachedManagedChannel.shutdown();
      try {
        boolean ignored = cachedManagedChannel.awaitTermination(1L, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
      }
    }
  }
}
