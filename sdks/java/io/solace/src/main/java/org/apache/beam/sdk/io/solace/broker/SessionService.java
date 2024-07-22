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

import com.solacesystems.jcsmp.JCSMPProperties;
import java.io.Serializable;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SessionService interface provides a set of methods for managing a session with the Solace
 * messaging system. It allows for establishing a connection, creating a message-receiver object,
 * checking if the connection is closed or not, and gracefully closing the session.
 *
 * <p>Override this class and the method {@link #initializeSessionProperties(JCSMPProperties)} with
 * your specific properties, including all those related to authentication.
 *
 * <p>The connector will call the method only once per session created, so you can perform
 * relatively heavy operations in that method (e.g. connect to a store or vault to retrieve
 * credentials).
 *
 * <p>There are some default properties that are set by default and can be overridden in this
 * provider, that are relevant for the writer connector, and not used in the case of the read
 * connector (since they are not necessary for reading):
 *
 * <ul>
 *   <li>VPN_NAME: default
 *   <li>GENERATE_SEND_TIMESTAMPS: true
 *   <li>PUB_MULTI_THREAD: true
 * </ul>
 *
 * <p>The connector overrides other properties, regardless of what this provider sends to the
 * connector. Those properties are the following. Again, these properties are only relevant for the
 * write connector.
 *
 * <ul>
 *   <li>PUB_ACK_WINDOW_SIZE
 *   <li>MESSAGE_CALLBACK_ON_REACTOR
 * </ul>
 *
 * Those properties are set by the connector based on the values of {@link
 * org.apache.beam.sdk.io.solace.SolaceIO.Write#withWriterType(SolaceIO.WriterType)} and {@link
 * org.apache.beam.sdk.io.solace.SolaceIO.Write#withSubmissionMode(SolaceIO.SubmissionMode)}.
 *
 * <p>The method will always run in a worker thread or task, and not in the driver program. If you
 * need to access any resource to set the properties, you need to make sure that the worker has the
 * network connectivity required for that, and that any credential or configuration is passed to the
 * provider through the constructor.
 *
 * <p>The connector ensures that no two threads will be calling that method at the same time, so you
 * don't have to take any specific precautions to avoid race conditions.
 *
 * <p>For basic authentication, use {@link BasicAuthJcsmpSessionService} and {@link
 * BasicAuthJcsmpSessionServiceFactory}.
 *
 * <p>For other situations, you need to extend this class. For instance:
 *
 * <pre>{@code
 * public class MySessionService extends SessionService {
 *   private final String authToken;
 *
 *   public MySessionService(String token) {
 *    this.oauthToken = token;
 *    ...
 *   }
 *
 *   {@literal }@Override
 *   public JCSMPProperties initializeSessionProperties(JCSMPProperties baseProps) {
 *     baseProps.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2);
 *     baseProps.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, authToken);
 *     return props;
 *   }
 *
 *   {@literal }@Override
 *   public void connect() {
 *       ...
 *   }
 *
 *   ...
 * }
 * }</pre>
 */
public abstract class SessionService implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SessionService.class);

  public static final String DEFAULT_VPN_NAME = "default";

  private static final int STREAMING_PUB_ACK_WINDOW = 50;
  private static final int BATCHED_PUB_ACK_WINDOW = 255;

  /**
   * Establishes a connection to the service. This could involve providing connection details like
   * host, port, VPN name, username, and password.
   */
  public abstract void connect();

  /** Gracefully closes the connection to the service. */
  public abstract void close();

  /**
   * Checks whether the connection to the service is currently closed. This method is called when an
   * `UnboundedSolaceReader` is starting to read messages - a session will be created if this
   * returns true.
   */
  public abstract boolean isClosed();

  /**
   * Creates a MessageReceiver object for receiving messages from Solace. Typically, this object is
   * created from the session instance.
   */
  public abstract MessageReceiver createReceiver();

  /**
   * Override this method and provide your specific properties, including all those related to
   * authentication, and possibly others too. The {@code}baseProperties{@code} parameter sets the
   * Solace VPN to "default" if none is specified.
   *
   * <p>You should add your properties to the parameter {@code}baseProperties{@code}, and return the
   * result.
   *
   * <p>The method will be used whenever the session needs to be created or refreshed. If you are
   * setting credentials with expiration, just make sure that the latest available credentials (e.g.
   * renewed token) are set when the method is called.
   *
   * <p>For a list of all the properties that can be set, please check the following link:
   *
   * <ul>
   *   <li><a
   *       href="https://docs.solace.com/API-Developer-Online-Ref-Documentation/java/com/solacesystems/jcsmp/JCSMPProperties.html">https://docs.solace.com/API-Developer-Online-Ref-Documentation/java/com/solacesystems/jcsmp/JCSMPProperties.html</a>
   * </ul>
   */
  public abstract JCSMPProperties initializeSessionProperties(JCSMPProperties baseProperties);

  /**
   * This method will be called by the write connector when a new session is started.
   *
   * <p>This call will happen in the worker, so you need to make sure that the worker has access to
   * the resources you need to set the properties.
   *
   * <p>The call will happen only once per session initialization. Typically, that will be when the
   * worker and the client are created. But if for any reason the session is lost (e.g. expired auth
   * token), this method will be called again.
   */
  public final JCSMPProperties initializeWriteSessionProperties(SolaceIO.SubmissionMode mode) {
    JCSMPProperties jcsmpProperties = initializeSessionProperties(getDefaultProperties());
    return overrideConnectorProperties(jcsmpProperties, mode);
  }

  private static JCSMPProperties getDefaultProperties() {
    JCSMPProperties props = new JCSMPProperties();
    props.setProperty(JCSMPProperties.VPN_NAME, DEFAULT_VPN_NAME);
    // Outgoing messages will have a sender timestamp field populated
    props.setProperty(JCSMPProperties.GENERATE_SEND_TIMESTAMPS, true);
    // Make XMLProducer safe to access from several threads. This is the default value, setting
    // it just in case.
    props.setProperty(JCSMPProperties.PUB_MULTI_THREAD, true);

    return props;
  }

  /**
   * This method overrides some properties for the broker session to prevent misconfiguration,
   * taking into account how the write connector works.
   */
  private static JCSMPProperties overrideConnectorProperties(
      JCSMPProperties props, SolaceIO.SubmissionMode mode) {

    // PUB_ACK_WINDOW_SIZE heavily affects performance when publishing persistent
    // messages. It can be a value between 1 and 255. This is the batch size for the ack
    // received from Solace. A value of 1 will have the lowest latency, but a very low
    // throughput and a monumental backpressure.

    // This controls how the messages are sent to Solace
    if (mode == SolaceIO.SubmissionMode.HIGHER_THROUGHPUT) {
      // Create a parallel thread and a queue to send the messages

      Boolean msgCbProp = props.getBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR);
      if (msgCbProp != null && msgCbProp) {
        LOG.warn(
            "SolaceIO.Write: Overriding MESSAGE_CALLBACK_ON_REACTOR to false since"
                + " HIGHER_THROUGHPUT mode was selected");
      }

      props.setProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR, false);

      Integer ackWindowSize = props.getIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE);
      if ((ackWindowSize != null && ackWindowSize != BATCHED_PUB_ACK_WINDOW)) {
        LOG.warn(
            String.format(
                "SolaceIO.Write: Overriding PUB_ACK_WINDOW_SIZE to %d since"
                    + " HIGHER_THROUGHPUT mode was selected",
                BATCHED_PUB_ACK_WINDOW));
      }
      props.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, BATCHED_PUB_ACK_WINDOW);
    } else {
      // Send from the same thread where the produced is being called. This offers the lowest
      // latency, but a low throughput too.
      Boolean msgCbProp = props.getBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR);
      if (msgCbProp != null && !msgCbProp) {
        LOG.warn(
            "SolaceIO.Write: Overriding MESSAGE_CALLBACK_ON_REACTOR to true since"
                + " LOWER_LATENCY mode was selected");
      }

      props.setProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR, true);

      Integer ackWindowSize = props.getIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE);
      if ((ackWindowSize != null && ackWindowSize != STREAMING_PUB_ACK_WINDOW)) {
        LOG.warn(
            String.format(
                "SolaceIO.Write: Overriding PUB_ACK_WINDOW_SIZE to %d since"
                    + " LOWER_LATENCY mode was selected",
                STREAMING_PUB_ACK_WINDOW));
      }

      props.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, STREAMING_PUB_ACK_WINDOW);
    }
    return props;
  }
}
