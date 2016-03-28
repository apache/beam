/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.common.base.Preconditions;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A helper class for talking to pub/sub via grpc.
 */
class PubsubGrpcClient implements AutoCloseable {
  private static final String PUBSUB_ADDRESS = "pubsub.googleapis.com";
  private static final int PUBSUB_PORT = 443;
  private static final List<String> PUBSUB_SCOPES =
      Collections.singletonList("https://www.googleapis.com/auth/pubsub");

  /**
   * Timeout for grpc calls (in s).
   */
  private static final int TIMEOUT_S = 15;

  /**
   * Underlying netty channel, or null if closed.
   */
  @Nullable
  private ManagedChannel publisherChannel;

  /**
   * Credentials determined from options and environment.
   */
  private GoogleCredentials credentials;

  /**
   * Cached stubs, or null if not cached.
   */
  @Nullable
  private PublisherGrpc.PublisherBlockingStub cachedPublisherStub;
  private SubscriberGrpc.SubscriberBlockingStub cachedSubscriberStub;

  private PubsubGrpcClient(ManagedChannel publisherChannel, GoogleCredentials credentials) {
    this.publisherChannel = publisherChannel;
    this.credentials = credentials;
  }

  /**
   * Construct a new pub/sub grpc client. It should be closed via {@link #close} in order
   * to ensure tidy cleanup of underlying netty resources.
   */
  static PubsubGrpcClient newClient(GcpOptions options)
      throws IOException, GeneralSecurityException {
    ManagedChannel channel = NettyChannelBuilder
        .forAddress(PUBSUB_ADDRESS, PUBSUB_PORT)
        .negotiationType(NegotiationType.TLS)
        .sslContext(GrpcSslContexts.forClient().ciphers(null).build())
        .build();
    // TODO: GcpOptions needs to support building com.google.auth.oauth2.Credentials from the
    // various command line options. It currently only supports the older
    // com.google.api.client.auth.oauth2.Credentials.
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    return new PubsubGrpcClient(channel, credentials);
  }

  /**
   * Gracefully close the underlying netty channel.
   */
  @Override
  public void close() {
    Preconditions.checkState(publisherChannel != null, "Client has already been closed");
    publisherChannel.shutdown();
    try {
      publisherChannel.awaitTermination(TIMEOUT_S, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // Ignore.
      Thread.currentThread().interrupt();
    }
    publisherChannel = null;
    cachedPublisherStub = null;
    cachedSubscriberStub = null;
  }

  /**
   * Return channel with interceptor for returning credentials.
   */
  private Channel newChannel() throws IOException {
    Preconditions.checkState(publisherChannel != null, "PubsubGrpcClient has been closed");
    ClientAuthInterceptor interceptor =
        new ClientAuthInterceptor(credentials, Executors.newSingleThreadExecutor());
    return ClientInterceptors.intercept(publisherChannel, interceptor);
  }

  /**
   * Return a stub for making a publish request with a timeout.
   */
  private PublisherGrpc.PublisherBlockingStub publisherStub() throws IOException {
    if (cachedPublisherStub == null) {
      cachedPublisherStub = PublisherGrpc.newBlockingStub(newChannel());
    }
    return cachedPublisherStub.withDeadlineAfter(TIMEOUT_S, TimeUnit.SECONDS);
  }

  /**
   * Return a stub for making a subscribe request with a timeout.
   */
  private SubscriberGrpc.SubscriberBlockingStub subscriberStub() throws IOException {
    if (cachedSubscriberStub == null) {
      cachedSubscriberStub = SubscriberGrpc.newBlockingStub(newChannel());
    }
    return cachedSubscriberStub.withDeadlineAfter(TIMEOUT_S, TimeUnit.SECONDS);
  }

  /**
   * The following are pass-through.
   */
  PublishResponse publish(PublishRequest request) throws IOException {
    return publisherStub().publish(request);
  }

  Empty acknowledge(AcknowledgeRequest request) throws IOException {
    return subscriberStub().acknowledge(request);
  }

  Empty modifyAckDeadline(ModifyAckDeadlineRequest request) throws IOException {
    return subscriberStub().modifyAckDeadline(request);
  }

  PullResponse pull(PullRequest request) throws IOException {
    return subscriberStub().pull(request);
  }
}
