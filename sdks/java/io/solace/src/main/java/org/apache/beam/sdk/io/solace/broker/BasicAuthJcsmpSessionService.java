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

import com.google.cloud.dataflow.dce.io.solace.RetryCallableManager;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import java.io.IOException;
import java.util.Set;

public class BasicAuthJcsmpSessionService implements SessionService {
    private final String queueName;
    private final String host;
    private final String username;
    private final String password;
    private final String vpnName;
    private JCSMPSession jcsmpSession;
    private final RetryCallableManager retryCallableManager = RetryCallableManager.create();

    public BasicAuthJcsmpSessionService(
            String queueName, String host, String username, String password, String vpnName) {
        this.queueName = queueName;
        this.host = host;
        this.username = username;
        this.password = password;
        this.vpnName = vpnName;
    }

    @Override
    public void connect() {
        retryCallableManager.retryCallable(this::connectSession, Set.of(JCSMPException.class));
    }

    @Override
    public void close() {
        if (jcsmpSession != null && !jcsmpSession.isClosed()) {
            retryCallableManager.retryCallable(
                    () -> {
                        jcsmpSession.closeSession();
                        return 0;
                    },
                    Set.of(IOException.class));
        }
    }

    @Override
    public MessageReceiver createReceiver() {
        return retryCallableManager.retryCallable(
                this::createFlowReceiver, Set.of(JCSMPException.class));
    }

    @Override
    public boolean isClosed() {
        return jcsmpSession == null || jcsmpSession.isClosed();
    }

    private MessageReceiver createFlowReceiver() throws JCSMPException {
        if (isClosed()) {
            connectSession();
        }

        Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);

        ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
        flowProperties.setEndpoint(queue);
        flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

        EndpointProperties endpointProperties = new EndpointProperties();
        endpointProperties.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

        return new SolaceMessageReceiver(
                jcsmpSession.createFlow(null, flowProperties, endpointProperties));
    }

    private int connectSession() throws JCSMPException {
        if (jcsmpSession == null) {
            jcsmpSession = createSessionObject();
        }
        jcsmpSession.connect();
        return 0;
    }

    private JCSMPSession createSessionObject() throws InvalidPropertiesException {
        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, host);
        properties.setProperty(JCSMPProperties.USERNAME, username);
        properties.setProperty(JCSMPProperties.PASSWORD, password);
        properties.setProperty(JCSMPProperties.VPN_NAME, vpnName);

        return JCSMPFactory.onlyInstance().createSession(properties);
    }
}
