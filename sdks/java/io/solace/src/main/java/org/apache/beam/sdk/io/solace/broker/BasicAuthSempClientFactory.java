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
package org.apache.beam.sdk.io.solace.broker;

import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.solace.SerializableSupplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
public abstract class BasicAuthSempClientFactory implements SempClientFactory {

  public abstract String host();

  public abstract String username();

  public abstract String password();

  public abstract String vpnName();

  public abstract @Nullable SerializableSupplier<HttpRequestFactory> httpRequestFactorySupplier();

  public static Builder builder() {
    return new AutoValue_BasicAuthSempClientFactory.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    /** Set Solace host, format: [Protocol://]Host[:Port]. */
    public abstract Builder host(String host);

    /** Set Solace username. */
    public abstract Builder username(String username);
    /** Set Solace password. */
    public abstract Builder password(String password);

    /** Set Solace vpn name. */
    public abstract Builder vpnName(String vpnName);

    @VisibleForTesting
    abstract Builder httpRequestFactorySupplier(
        SerializableSupplier<HttpRequestFactory> httpRequestFactorySupplier);

    public abstract BasicAuthSempClientFactory build();
  }

  @Override
  public SempClient create() {
    return new BasicAuthSempClient(
        host(), username(), password(), vpnName(), getHttpRequestFactorySupplier());
  }

  @SuppressWarnings("return")
  private @NonNull SerializableSupplier<HttpRequestFactory> getHttpRequestFactorySupplier() {
    SerializableSupplier<HttpRequestFactory> httpRequestSupplier = httpRequestFactorySupplier();
    return httpRequestSupplier != null
        ? httpRequestSupplier
        : () -> new NetHttpTransport().createRequestFactory();
  }
}
