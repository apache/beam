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

import com.google.common.base.Preconditions;

public class BasicAuthJcsmpSessionServiceFactory extends SessionServiceFactory {
    private final String host;
    private final String username;
    private final String password;
    private final String vpnName;

    private BasicAuthJcsmpSessionServiceFactory(
            String host, String username, String password, String vpnName) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.vpnName = vpnName;
    }

    public static BasicAuthJcsmpSessionServiceFactoryBuilder builder() {
        return new BasicAuthJcsmpSessionServiceFactoryBuilder();
    }

    @Override
    public SessionService create() {
        Preconditions.checkState(queue != null, "SolaceIO.Read: Queue is not set.");
        return new BasicAuthJcsmpSessionService(queue.getName(), host, username, password, vpnName);
    }

    public static class BasicAuthJcsmpSessionServiceFactoryBuilder {

        private String host;
        private String username;
        private String password;
        private String vpnName;

        public BasicAuthJcsmpSessionServiceFactoryBuilder withHost(String host) {
            this.host = host;
            return this;
        }

        public BasicAuthJcsmpSessionServiceFactoryBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public BasicAuthJcsmpSessionServiceFactoryBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public BasicAuthJcsmpSessionServiceFactoryBuilder withVpnName(String vpnName) {
            this.vpnName = vpnName;
            return this;
        }

        public BasicAuthJcsmpSessionServiceFactory build() {
            return new BasicAuthJcsmpSessionServiceFactory(host, username, password, vpnName);
        }
    }
}
