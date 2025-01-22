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

import com.google.auto.value.AutoValue;
import com.solacesystems.jcsmp.JCSMPProperties;

/**
 * A factory for creating {@link JcsmpSessionService} instances. Extends {@link
 * SessionServiceFactory}.
 *
 * <p>This factory provides a way to create {@link JcsmpSessionService} that use Basic
 * Authentication.
 */
@AutoValue
public abstract class BasicAuthJcsmpSessionServiceFactory extends SessionServiceFactory {
  /** The host name or IP address of the Solace broker. Format: Host[:Port] */
  public abstract String host();

  /** The username to use for authentication. */
  public abstract String username();

  /** The password to use for authentication. */
  public abstract String password();

  /** The name of the VPN to connect to. */
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
    JCSMPProperties jcsmpProperties = new JCSMPProperties();
    jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, vpnName());
    jcsmpProperties.setProperty(
        JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_BASIC);
    jcsmpProperties.setProperty(JCSMPProperties.USERNAME, username());
    jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, password());
    jcsmpProperties.setProperty(JCSMPProperties.HOST, host());

    return JcsmpSessionService.create(jcsmpProperties, getQueue());
  }
}
