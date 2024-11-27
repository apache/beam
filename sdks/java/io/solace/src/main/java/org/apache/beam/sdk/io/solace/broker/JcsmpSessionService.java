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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageProducer;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.solace.RetryCallableManager;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.data.Solace.PublishResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/**
 * A class that manages a connection to a Solace broker using basic authentication.
 *
 * <p>This class provides a way to connect to a Solace broker and receive messages from a queue.
 */
@AutoValue
public abstract class JcsmpSessionService extends SessionService {

  /** JCSMP properties used to establish the connection. */
  abstract JCSMPProperties jcsmpProperties();

  /** The Queue to receive messages from. */
  abstract @Nullable Queue queue();

  @Nullable private transient JCSMPSession jcsmpSession;
  @Nullable private transient MessageReceiver messageReceiver;
  @Nullable private transient MessageProducer messageProducer;
  private final java.util.Queue<PublishResult> publishedResultsQueue =
      new ConcurrentLinkedQueue<>();
  private final RetryCallableManager retryCallableManager = RetryCallableManager.create();

  public static JcsmpSessionService create(JCSMPProperties jcsmpProperties, @Nullable Queue queue) {
    return new AutoValue_JcsmpSessionService(jcsmpProperties, queue);
  }

  @Override
  public JCSMPProperties getSessionProperties() {
    return jcsmpProperties();
  }

  @Override
  public void connect() {
    retryCallableManager.retryCallable(
        this::connectReadSession, ImmutableSet.of(JCSMPException.class));
  }

  @Override
  public void close() {
    retryCallableManager.retryCallable(
        () -> {
          if (messageReceiver != null) {
            messageReceiver.close();
          }
          if (jcsmpSession != null) {
            checkStateNotNull(jcsmpSession).closeSession();
          }
          return 0;
        },
        ImmutableSet.of(IOException.class));
  }

  @Override
  public MessageReceiver getReceiver() {
    if (this.messageReceiver == null) {
      this.messageReceiver =
          retryCallableManager.retryCallable(
              this::createFlowReceiver, ImmutableSet.of(JCSMPException.class));
      this.messageReceiver.start();
    }
    return checkStateNotNull(this.messageReceiver);
  }

  @Override
  public MessageProducer getInitializedProducer(SubmissionMode submissionMode) {
    if (this.messageProducer == null || this.messageProducer.isClosed()) {
      Callable<MessageProducer> create = () -> createXMLMessageProducer(submissionMode);
      this.messageProducer =
          retryCallableManager.retryCallable(create, ImmutableSet.of(JCSMPException.class));
    }
    return checkStateNotNull(this.messageProducer);
  }

  @Override
  public java.util.Queue<PublishResult> getPublishedResultsQueue() {
    return publishedResultsQueue;
  }

  private MessageProducer createXMLMessageProducer(SubmissionMode submissionMode)
      throws JCSMPException, IOException {

    if (jcsmpSession == null) {
      connectWriteSession(submissionMode);
    }

    @SuppressWarnings("nullness")
    Callable<XMLMessageProducer> initProducer =
        () ->
            Objects.requireNonNull(jcsmpSession)
                .getMessageProducer(new PublishResultHandler(publishedResultsQueue));

    XMLMessageProducer producer =
        retryCallableManager.retryCallable(initProducer, ImmutableSet.of(JCSMPException.class));
    if (producer == null) {
      throw new IOException("SolaceIO.Write: Could not create producer, producer object is null");
    }
    return new SolaceMessageProducer(producer);
  }

  private MessageReceiver createFlowReceiver() throws JCSMPException, IOException {
    if (isClosed()) {
      connectReadSession();
    }

    Queue queue = checkStateNotNull(queue(), "SolaceIO.Read: Queue is not set.");

    ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
    flowProperties.setEndpoint(queue);
    flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

    EndpointProperties endpointProperties = new EndpointProperties();
    endpointProperties.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
    if (jcsmpSession != null) {
      return new SolaceMessageReceiver(
          createFlowReceiver(jcsmpSession, flowProperties, endpointProperties));
    }
    throw new IOException(
        "SolaceIO.Read: Could not create a receiver from the Jcsmp session: session object is"
            + " null.");
  }

  // The `@SuppressWarning` is needed here, because the checkerframework reports an error for the
  // first argument of the `createFlow` being null, even though the documentation allows it:
  // https://docs.solace.com/API-Developer-Online-Ref-Documentation/java/com/solacesystems/jcsmp/JCSMPSession.html#createFlow-com.solacesystems.jcsmp.XMLMessageListener-com.solacesystems.jcsmp.ConsumerFlowProperties-com.solacesystems.jcsmp.EndpointProperties-
  @SuppressWarnings("nullness")
  private static FlowReceiver createFlowReceiver(
      JCSMPSession jcsmpSession,
      ConsumerFlowProperties flowProperties,
      EndpointProperties endpointProperties)
      throws JCSMPException {
    return jcsmpSession.createFlow(null, flowProperties, endpointProperties);
  }

  private int connectReadSession() throws JCSMPException {
    if (jcsmpSession == null) {
      jcsmpSession = createReadSessionObject();
    }
    jcsmpSession.connect();
    return 0;
  }

  private int connectWriteSession(SubmissionMode mode) throws JCSMPException {
    if (jcsmpSession == null) {
      jcsmpSession = createWriteSessionObject(mode);
    }
    jcsmpSession.connect();
    return 0;
  }

  private JCSMPSession createReadSessionObject() throws InvalidPropertiesException {
    return JCSMPFactory.onlyInstance().createSession(jcsmpProperties());
  }

  private JCSMPSession createWriteSessionObject(SubmissionMode mode)
      throws InvalidPropertiesException {
    return JCSMPFactory.onlyInstance().createSession(initializeWriteSessionProperties(mode));
  }
}
