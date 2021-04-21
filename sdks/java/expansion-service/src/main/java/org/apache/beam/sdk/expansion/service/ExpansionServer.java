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
package org.apache.beam.sdk.expansion.service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.netty.NettyServerBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/** A {@link Server gRPC Server} for an ExpansionService. */
public class ExpansionServer implements AutoCloseable {
  /**
   * Create a {@link ExpansionServer} for the provided ExpansionService running on an arbitrary
   * port.
   *
   * <p>If port is 0, a free port will be assigned.
   */
  public static ExpansionServer create(ExpansionService service, String host, int port)
      throws IOException {
    return new ExpansionServer(service, host, port);
  }

  private final String host;
  private final int port;
  private final Server server;
  private final ExpansionService service;

  private ExpansionServer(ExpansionService service, String host, int port) throws IOException {
    this.service = Preconditions.checkNotNull(service);
    this.host = Preconditions.checkNotNull(host);
    this.server =
        NettyServerBuilder.forAddress(new InetSocketAddress(host, port))
            .addService(service)
            .addService(new ArtifactRetrievalService())
            .build()
            .start();
    this.port = server.getPort();
  }

  /** Get the host that this {@link ExpansionServer} is bound to. */
  public String getHost() {
    return host;
  }

  /** Get the port that this {@link ExpansionServer} is bound to. */
  public int getPort() {
    return port;
  }

  @Override
  public void close() throws Exception {
    try {
      // The server has been closed, and should not respond to any new incoming calls.
      server.shutdown();
      service.close();
      server.awaitTermination(60, TimeUnit.SECONDS);
    } finally {
      server.shutdownNow();
      server.awaitTermination();
    }
  }
}
