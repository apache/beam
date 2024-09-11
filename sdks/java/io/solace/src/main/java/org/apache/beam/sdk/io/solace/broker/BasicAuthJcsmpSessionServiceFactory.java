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

import static org.apache.beam.sdk.io.solace.broker.SessionService.DEFAULT_VPN_NAME;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;

/**
 * A factory for creating {@link BasicAuthJcsmpSessionService} instances. Extends {@link
 * SessionServiceFactory}.
 *
 * <p>This factory provides a way to create {@link BasicAuthJcsmpSessionService} instances with
 * authenticate to Solace with Basic Authentication.
 */
@AutoValue
public abstract class BasicAuthJcsmpSessionServiceFactory extends SessionServiceFactory {
  public abstract String host();

  public abstract String username();

  public abstract String password();

  public abstract String vpnName();

  public static Builder builder() {
    return new AutoValue_BasicAuthJcsmpSessionServiceFactory.Builder().vpnName(DEFAULT_VPN_NAME);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * Set Solace host, format: Host[:Port] e.g. "12.34.56.78", or "[fe80::1]", or
     * "12.34.56.78:4444".
     */
    public abstract Builder host(String host);

    /** Set Solace username. */
    public abstract Builder username(String username);
    /** Set Solace password. */
    public abstract Builder password(String password);

    /** Set Solace vpn name. */
    public abstract Builder vpnName(String vpnName);

    public abstract BasicAuthJcsmpSessionServiceFactory build();
  }

  @Override
  public SessionService create() {
    return new BasicAuthJcsmpSessionService(
        checkStateNotNull(queue, "SolaceIO.Read: Queue is not set.").getName(),
        host(),
        username(),
        password(),
        vpnName());
  }
}
