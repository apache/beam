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

import com.google.common.util.concurrent.SettableFuture;
import com.solacesystems.jcsmp.JCSMPProperties;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.data.Solace.PublishResult;
import org.apache.beam.sdk.io.solace.write.PublishPhaser;
import org.checkerframework.checker.nullness.qual.Nullable;
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
 * <p>For other situations, you need to extend this class and implement the `equals` method, so two
 * instances of your class can be compared by value. We recommend using AutoValue for that. For
 * instance:
 *
 * <pre>{@code
 * {@literal }@AutoValue
 * public class MySessionService extends SessionService {
 *   abstract String authToken();
 *
 *   public static MySessionService create(String authToken) {
 *       return new AutoValue_MySessionService(authToken);
 *   }
 *
 *   {@literal }@Override
 *   public JCSMPProperties initializeSessionProperties(JCSMPProperties baseProps) {
 *     baseProps.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2);
 *     baseProps.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, authToken());
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

  private static final int TESTING_PUB_ACK_WINDOW = 1;
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
   * Returns a MessageReceiver object for receiving messages from Solace. If it is the first time
   * this method is used, the receiver is created from the session instance, otherwise it returns
   * the receiver created initially.
   */
  public abstract MessageReceiver getReceiver();

  /**
   * Returns a MessageProducer object for publishing messages to Solace. If it is the first time
   * this method is used, the producer is created from the session instance, otherwise it returns
   * the producer created initially.
   */
  public abstract MessageProducer getInitializedProducer(SubmissionMode mode);

  /**
   * todo fix this doc Returns the {@link ConcurrentHashMap<String, SettableFuture<PublishResult>>}
   * instance associated with this session, with the asynchronously received callbacks from Solace
   * for message publications. The queue implementation has to be thread-safe for production
   * use-cases.
   */
  public abstract ConcurrentHashMap<String, PublishPhaser> getPublishedResults();

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
   * You need to override this method to be able to compare these objects by value. We recommend
   * using AutoValue for that.
   */
  @Override
  public abstract boolean equals(@Nullable Object other);

  /**
   * You need to override this method to be able to compare these objects by value. We recommend
   * using AutoValue for that.
   */
  @Override
  public abstract int hashCode();

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

    // Retrieve current values of the properties
    Boolean msgCbProp = props.getBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR);
    Integer ackWindowSize = props.getIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE);

    switch (mode) {
      case HIGHER_THROUGHPUT:
        // Check if it was set by user, show override warning
        if (msgCbProp != null && msgCbProp) {
          LOG.warn(
              "SolaceIO.Write: Overriding MESSAGE_CALLBACK_ON_REACTOR to false since"
                  + " HIGHER_THROUGHPUT mode was selected");
        }
        if ((ackWindowSize != null && ackWindowSize != BATCHED_PUB_ACK_WINDOW)) {
          LOG.warn(
              String.format(
                  "SolaceIO.Write: Overriding PUB_ACK_WINDOW_SIZE to %d since"
                      + " HIGHER_THROUGHPUT mode was selected",
                  BATCHED_PUB_ACK_WINDOW));
        }

        // Override the properties
        // Use a dedicated thread for callbacks, increase the ack window size
        props.setProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR, false);
        props.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, BATCHED_PUB_ACK_WINDOW);
        LOG.info(
            "SolaceIO.Write: Using HIGHER_THROUGHPUT mode, MESSAGE_CALLBACK_ON_REACTOR is FALSE,"
                + " PUB_ACK_WINDOW_SIZE is {}",
            BATCHED_PUB_ACK_WINDOW);
        break;
      case LOWER_LATENCY:
        // Check if it was set by user, show override warning
        if (msgCbProp != null && !msgCbProp) {
          LOG.warn(
              "SolaceIO.Write: Overriding MESSAGE_CALLBACK_ON_REACTOR to true since"
                  + " LOWER_LATENCY mode was selected");
        }

        if ((ackWindowSize != null && ackWindowSize != STREAMING_PUB_ACK_WINDOW)) {
          LOG.warn(
              String.format(
                  "SolaceIO.Write: Overriding PUB_ACK_WINDOW_SIZE to %d since"
                      + " LOWER_LATENCY mode was selected",
                  STREAMING_PUB_ACK_WINDOW));
        }

        // Override the properties
        // Send from the same thread where the produced is being called. This offers the lowest
        // latency, but a low throughput too.
        props.setProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR, true);
        props.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, STREAMING_PUB_ACK_WINDOW);
        LOG.info(
            "SolaceIO.Write: Using LOWER_LATENCY mode, MESSAGE_CALLBACK_ON_REACTOR is TRUE,"
                + " PUB_ACK_WINDOW_SIZE is {}",
            STREAMING_PUB_ACK_WINDOW);

        break;
      case CUSTOM:
        LOG.info(
            " SolaceIO.Write: Using the custom JCSMP properties set by the user. No property has"
                + " been overridden by the connector.");
        break;
      case TESTING:
        LOG.warn(
            "SolaceIO.Write: Overriding JCSMP properties for testing. **IF THIS IS AN"
                + " ACTUAL PIPELINE, CHANGE THE SUBMISSION MODE TO HIGHER_THROUGHPUT "
                + "OR LOWER_LATENCY.**");
        // Minimize multi-threading for testing
        props.setProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR, true);
        props.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, TESTING_PUB_ACK_WINDOW);
        break;
      default:
        LOG.error(
            "SolaceIO.Write: no submission mode is selected. Set the submission mode to"
                + " HIGHER_THROUGHPUT or LOWER_LATENCY;");
    }
    return props;
  }
}
