/*
 * Copyright 2024 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.dataflow.dce.io.solace.broker;

import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.dataflow.dce.io.solace.SerializableSupplier;
import com.google.common.base.Preconditions;
import org.apache.arrow.util.VisibleForTesting;

public class BasicAuthSempClientFactory implements SempClientFactory {

    private final String host;
    private final String username;
    private final String password;
    private final String vpnName;
    private final SerializableSupplier<HttpRequestFactory> httpRequestFactorySupplier;

    private BasicAuthSempClientFactory(
            String host,
            String username,
            String password,
            String vpnName,
            SerializableSupplier<HttpRequestFactory> httpRequestFactorySupplier) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.vpnName = vpnName;
        this.httpRequestFactorySupplier = httpRequestFactorySupplier;
    }

    public static BasicAuthSempAuthenticationFactoryBuilder builder() {
        return new BasicAuthSempAuthenticationFactoryBuilder()
                .withHttpRequestFactorySupplier(
                        () -> new NetHttpTransport().createRequestFactory());
    }

    @Override
    public SempClient create() {
        return new BasicAuthSempClient(
                host, username, password, vpnName, httpRequestFactorySupplier);
    }

    public static class BasicAuthSempAuthenticationFactoryBuilder {

        private String host;
        private String username;
        private String password;
        private String vpnName;
        private SerializableSupplier<HttpRequestFactory> httpRequestFactorySupplier;

        /** Set Solace host, format: Protocol://Host[:Port] */
        public BasicAuthSempAuthenticationFactoryBuilder withHost(String host) {
            this.host = host;
            return this;
        }

        /** Set Solace username */
        public BasicAuthSempAuthenticationFactoryBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        /** Set Solace password */
        public BasicAuthSempAuthenticationFactoryBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        /** Set Solace vpn name */
        public BasicAuthSempAuthenticationFactoryBuilder withVpnName(String vpnName) {
            this.vpnName = vpnName;
            return this;
        }

        @VisibleForTesting
        BasicAuthSempAuthenticationFactoryBuilder withHttpRequestFactorySupplier(
                SerializableSupplier<HttpRequestFactory> httpRequestFactorySupplier) {
            this.httpRequestFactorySupplier = httpRequestFactorySupplier;
            return this;
        }

        public BasicAuthSempClientFactory build() {
            // todo update name in the error string
            Preconditions.checkState(
                    host != null,
                    "SolaceIO: host in BasicAuthSempAuthenticationFactory can't be null. Set it"
                            + " with `withHost()` method.");
            Preconditions.checkState(
                    username != null,
                    "SolaceIO: username in BasicAuthSempAuthenticationFactory can't be null. Set it"
                            + " with `withUsername()` method.");
            Preconditions.checkState(
                    password != null,
                    "SolaceIO: password in BasicAuthSempAuthenticationFactory can't be null. Set it"
                            + " with `withPassword()` method.");
            Preconditions.checkState(
                    vpnName != null,
                    "SolaceIO: vpnName in BasicAuthSempAuthenticationFactory can't be null. Set it"
                            + " with `withVpnName()` method.");
            Preconditions.checkState(
                    httpRequestFactorySupplier != null,
                    "SolaceIO: httpRequestFactorySupplier in BasicAuthSempAuthenticationFactory"
                        + " can't be null. Set it with `withHttpRequestFactorySupplier()` method.");

            return new BasicAuthSempClientFactory(
                    host, username, password, vpnName, httpRequestFactorySupplier);
        }
    }
}
