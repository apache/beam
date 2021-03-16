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

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.Credentials;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.FirestoreOptions.EmulatorCredentials;
import com.google.cloud.firestore.spi.v1.FirestoreRpc;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.Map;
import javax.annotation.concurrent.Immutable;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.CounterFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.Sleeper;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Factory class for all stateful components used in the Firestore Connector.
 * <p/>
 * None of the components returned by any of these factory methods are serializable, this factory
 * functions to give a serialization friendly handle to create instances of these components.
 * <p/>
 * This class is stateless.
 */
@Immutable
class FirestoreStatefulComponentFactory implements Serializable {

  static final FirestoreStatefulComponentFactory INSTANCE = new FirestoreStatefulComponentFactory();

  private FirestoreStatefulComponentFactory() {
  }

  /**
   * Given a {@link PipelineOptions}, return a pre-configured {@link FirestoreOptions.Builder} with
   * values set based on those options.
   * <p/>
   * The provided {@link PipelineOptions} is expected to provide {@link FirestoreIOOptions} and
   * {@link org.apache.beam.sdk.extensions.gcp.options.GcpOptions GcpOptions} if connecting to live
   * Firestore (i.e. not an emulator).
   * <p/>
   * The instance returned by this method is expected to bind to the lifecycle of a bundle.
   *
   * @param options The instance of options to read from
   * @return a new {@link FirestoreOptions.Builder} pre-configured with values from the provided
   * options
   */
  FirestoreOptions.Builder getFirestoreOptionsBuilder(PipelineOptions options) {
    FirestoreOptions.Builder builder = FirestoreOptions.getDefaultInstance().toBuilder()
        .setProjectId(options.as(GcpOptions.class).getProject())
        .setHeaderProvider(new FixedHeaderProvider() {
          @Override
          public Map<@NonNull String, @NonNull String> getHeaders() {
            String version = "x.y.z";  // TODO: How does beam manage version detection in project?
            return ImmutableMap.of("User-Agent", String.format("beam-sdk/%s", version));
          }
        });

    FirestoreIOOptions ioOptions = options.as(FirestoreIOOptions.class);
    String emulatorHostPort = ioOptions.getEmulatorHostPort();
    if (emulatorHostPort != null) {
      builder.setEmulatorHost(emulatorHostPort)
          .setCredentials(new EmulatorCredentials());
    } else {
      Credentials credential = options.as(GcpOptions.class).getGcpCredential();
      builder
          .setCredentials(credential)
          .setHost("batch-firestore.googleapis.com");
    }
    return builder;
  }

  /**
   * Given a {@link PipelineOptions}, return a pre-configured {@link FirestoreRpc} with values set
   * based on those options.
   * <p/>
   * The provided {@link PipelineOptions} is expected to provide {@link FirestoreIOOptions} and
   * {@link org.apache.beam.sdk.extensions.gcp.options.GcpOptions GcpOptions} if connecting to live
   * Firestore (i.e. not an emulator).
   * <p/>
   * The instance returned by this method is expected to bind to the lifecycle of a bundle.
   *
   * @param options The instance of options to read from
   * @return a new {@link FirestoreRpc} pre-configured with values from the provided options
   * @see #getFirestoreOptionsBuilder(PipelineOptions)
   */
  FirestoreRpc getFirestoreRpc(PipelineOptions options) {
    return (FirestoreRpc) getFirestoreOptionsBuilder(options)
        .build()
        .getRpc();
  }

  /**
   * Given a {@link RpcQosOptions}, return a new instance of {@link RpcQos}
   * <p/>
   * The instance returned by this method is expected to bind to the lifecycle of a worker, and
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
        CounterFactory.DEFAULT
    );
  }

}
