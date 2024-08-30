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
package org.apache.beam.sdk.io.gcp.firestore;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ClientContext;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.firestore.FirestoreOptions.EmulatorCredentials;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.cloud.firestore.v1.stub.FirestoreStub;
import com.google.cloud.firestore.v1.stub.GrpcFirestoreStub;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.Map;
import javax.annotation.concurrent.Immutable;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Factory class for all stateful components used in the Firestore Connector.
 *
 * <p>None of the components returned by any of these factory methods are serializable, this factory
 * functions to give a serialization friendly handle to create instances of these components.
 *
 * <p>This class is stateless.
 */
@Immutable
class FirestoreStatefulComponentFactory implements Serializable {

  static final FirestoreStatefulComponentFactory INSTANCE = new FirestoreStatefulComponentFactory();

  private FirestoreStatefulComponentFactory() {}

  /**
   * Given a {@link PipelineOptions}, return a pre-configured {@link FirestoreStub} with values set
   * based on those options.
   *
   * <p>The provided {@link PipelineOptions} is expected to provide {@link FirestoreOptions} and
   * {@link org.apache.beam.sdk.extensions.gcp.options.GcpOptions GcpOptions} for access to {@link
   * GcpOptions#getProject()}
   *
   * <p>The instance returned by this method is expected to bind to the lifecycle of a bundle.
   *
   * @param options The instance of options to read from
   * @return a new {@link FirestoreStub} pre-configured with values from the provided options
   */
  FirestoreStub getFirestoreStub(PipelineOptions options) {
    try {
      FirestoreSettings.Builder builder = FirestoreSettings.newBuilder();

      RetrySettings retrySettings = RetrySettings.newBuilder().setMaxAttempts(1).build();

      builder.applyToAllUnaryMethods(
          b -> {
            b.setRetrySettings(retrySettings);
            return null;
          });

      FirestoreOptions firestoreOptions = options.as(FirestoreOptions.class);
      String emulatorHostPort = firestoreOptions.getEmulatorHost();
      ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
      headers.put("User-Agent", options.getUserAgent());
      if (emulatorHostPort != null) {
        builder
            .setCredentialsProvider(FixedCredentialsProvider.create(new EmulatorCredentials()))
            .setEndpoint(emulatorHostPort)
            .setTransportChannelProvider(
                InstantiatingGrpcChannelProvider.newBuilder()
                    .setEndpoint(emulatorHostPort)
                    .setChannelConfigurator(c -> c.usePlaintext())
                    .build());
      } else {
        GcpOptions gcpOptions = options.as(GcpOptions.class);
        builder
            .setCredentialsProvider(FixedCredentialsProvider.create(gcpOptions.getGcpCredential()))
            .setEndpoint(firestoreOptions.getFirestoreHost());
        headers.put(
            "x-goog-request-params",
            "project_id="
                + gcpOptions.getProject()
                + "&database_id="
                + firestoreOptions.getFirestoreDb());
      }

      builder.setHeaderProvider(
          new FixedHeaderProvider() {
            @Override
            public Map<@NonNull String, @NonNull String> getHeaders() {
              return headers.build();
            }
          });

      ClientContext clientContext = ClientContext.create(builder.build());
      return GrpcFirestoreStub.create(clientContext);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Given a {@link RpcQosOptions}, return a new instance of {@link RpcQos}
   *
   * <p>The instance returned by this method is expected to bind to the lifecycle of a worker, and
   * specifically live longer than a single bundle.
   *
   * @param options The instance of options to read from
   * @return a new {@link RpcQos} based on the provided options
   */
  RpcQos getRpcQos(RpcQosOptions options) {
    return new RpcQosImpl(
        options,
        new SecureRandom(),
        Sleeper.DEFAULT,
        CounterFactory.DEFAULT,
        DistributionFactory.DEFAULT);
  }
}
