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
package org.apache.beam.sdk.io.solace.write.properties;

import com.google.auto.value.AutoValue;
import com.solacesystems.jcsmp.JCSMPProperties;

/**
 * This is a convenience class to use with basic authentication for the {@link
 * org.apache.beam.sdk.io.solace.SolaceIO.Write} connector.
 *
 * <p>This class is used by the connector to initialize the client session. The connector calls the
 * {@link #initializeSessionProperties(JCSMPProperties)} and pass that object (with some additions)
 * to the broker to create a session.
 *
 * <p>In this case, the properties that are set are those related to the authentication and the host
 * details where the broker is located. The host should include the full host and port details, if
 * the port used is not the default (55555).
 *
 * <p>Example of how to create the provider object:
 *
 * <pre>{@code
 * BasicAuthenticationProvider propsFactory =
 *     BasicAuthenticationProvider.builder()
 *         .username("username")
 *         .password("password")
 *         .host("host:port")
 *         .build();
 * }</pre>
 */
@AutoValue
public abstract class BasicAuthenticationProvider extends SessionPropertiesProvider {

  public abstract String username();

  public abstract String password();

  public abstract String host();

  public abstract String vpnName();

  public static Builder builder() {
    return new AutoValue_BasicAuthenticationProvider.Builder().vpnName(DEFAULT_VPN_NAME);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    /** Username to be used to authenticate with the broker */
    public abstract Builder username(String username);

    /** Password to be used to authenticate with the broker */
    public abstract Builder password(String password);

    /**
     * The location of the broker, including port details if it is not listening in the default port
     */
    public abstract Builder host(String host);

    /** Optional. Solace broker VPN name. If not set, "default" is used. */
    public abstract Builder vpnName(String vpnName);

    public abstract BasicAuthenticationProvider build();
  }

  @Override
  public JCSMPProperties initializeSessionProperties(JCSMPProperties baseProps) {
    baseProps.setProperty(JCSMPProperties.VPN_NAME, vpnName());

    baseProps.setProperty(
        JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_BASIC);
    baseProps.setProperty(JCSMPProperties.USERNAME, username());
    baseProps.setProperty(JCSMPProperties.PASSWORD, password());
    baseProps.setProperty(JCSMPProperties.HOST, host());
    return baseProps;
  }
}
