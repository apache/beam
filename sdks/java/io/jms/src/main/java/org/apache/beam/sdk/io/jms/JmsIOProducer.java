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
package org.apache.beam.sdk.io.jms;

import static org.apache.beam.sdk.io.jms.PublicationRetryPolicy.DEFAULT_PUBLICATION_RETRY_DURATION;

import java.io.IOException;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class JmsIOProducer<T> extends PTransform<PCollection<T>, WriteJmsResult<T>> {

  public static final String CONNECTION_ERRORS_METRIC_NAME = "connectionErrors";
  public static final String PUBLICATION_RETRIES_METRIC_NAME = "publicationRetries";
  public static final String JMS_IO_PRODUCER_METRIC_NAME = JmsIOProducer.class.getCanonicalName();

  private static final Logger LOG = LoggerFactory.getLogger(JmsIOProducer.class);
  private static final String PUBLISH_TO_JMS_STEP_NAME = "Publish to JMS";

  private final JmsIO.Write<T> spec;
  private final TupleTag<T> messagesTag;
  private final TupleTag<T> failedMessagesTag;

  JmsIOProducer(JmsIO.Write<T> spec) {
    this.spec = spec;
    this.messagesTag = new TupleTag<>();
    this.failedMessagesTag = new TupleTag<>();
  }

  @Override
  public WriteJmsResult<T> expand(PCollection<T> input) {
    PCollectionTuple failedPublishedMessages =
        input.apply(
            PUBLISH_TO_JMS_STEP_NAME,
            ParDo.of(new JmsIOProducerFn())
                .withOutputTags(messagesTag, TupleTagList.of(failedMessagesTag)));
    PCollection<T> failedMessages =
        failedPublishedMessages.get(failedMessagesTag).setCoder(input.getCoder());
    failedPublishedMessages.get(messagesTag).setCoder(input.getCoder());
    return WriteJmsResult.in(input.getPipeline(), failedMessagesTag, failedMessages);
  }

  private class JmsIOProducerFn extends DoFn<T, T> {

    private transient @Initialized Session session;
    private transient @Initialized Connection connection;
    private transient @Initialized Destination destination;
    private transient @Initialized MessageProducer producer;
    private transient @Initialized FluentBackoff retryPublicationBackoff;

    private final Counter connectionErrors =
        Metrics.counter(JMS_IO_PRODUCER_METRIC_NAME, CONNECTION_ERRORS_METRIC_NAME);
    private final Counter publicationRetries =
        Metrics.counter(JMS_IO_PRODUCER_METRIC_NAME, PUBLICATION_RETRIES_METRIC_NAME);

    @Setup
    public void setup() {
      retryPublicationBackoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(0)
              .withInitialBackoff(DEFAULT_PUBLICATION_RETRY_DURATION);

      if (spec.getRetryPublicationPolicy() != null) {
        retryPublicationBackoff =
            retryPublicationBackoff
                .withMaxRetries(spec.getRetryPublicationPolicy().maxPublicationAttempts())
                .withInitialBackoff(spec.getRetryPublicationPolicy().retryDuration());
      }
    }

    @StartBundle
    public void start() throws JMSException {
      if (producer == null) {
        ConnectionFactory connectionFactory = spec.getConnectionFactory();
        if (spec.getUsername() != null) {
          this.connection =
              connectionFactory.createConnection(spec.getUsername(), spec.getPassword());
        } else {
          this.connection = connectionFactory.createConnection();
        }
        this.connection.setExceptionListener(
            exception -> {
              if (spec.getConnectionFailedPredicate() != null
                  && spec.getConnectionFailedPredicate().apply(exception)) {
                LOG.error("Jms connection encountered the following exception:", exception);
                connectionErrors.inc();
                try {
                  restartJmsConnection();
                } catch (JMSException e) {
                  LOG.error("An error occurred while reconnecting:", e);
                }
              }
            });
        this.connection.start();
        // false means we don't use JMS transaction.
        this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        if (spec.getQueue() != null) {
          this.destination = session.createQueue(spec.getQueue());
        } else if (spec.getTopic() != null) {
          this.destination = session.createTopic(spec.getTopic());
        }
        this.producer = this.session.createProducer(this.destination);
      }
    }

    @ProcessElement
    public void processElement(@Element T input, ProcessContext context) {
      try {
        publishMessage(input, context);
      } catch (IOException | InterruptedException exception) {
        LOG.error("Error while publishing the message", exception);
        context.output(failedMessagesTag, input);
        Thread.currentThread().interrupt();
      }
    }

    private void publishMessage(T input, ProcessContext context)
        throws IOException, InterruptedException {
      Sleeper sleeper = Sleeper.DEFAULT;
      Destination destinationToSendTo = destination;
      BackOff backoff = retryPublicationBackoff.backoff();
      int publicationAttempt = 0;
      while (publicationAttempt >= 0) {
        publicationAttempt++;
        try {
          Message message = spec.getValueMapper().apply(input, session);
          if (spec.getTopicNameMapper() != null) {
            destinationToSendTo = session.createTopic(spec.getTopicNameMapper().apply(input));
          }
          producer.send(destinationToSendTo, message);
          publicationAttempt = -1;
        } catch (Exception exception) {
          if (spec.getRetryPublicationPolicy() == null || !BackOffUtils.next(sleeper, backoff)) {
            outputFailedMessage(input, context, destinationToSendTo, exception);
            LOG.debug("Message published to topic {}", destinationToSendTo);
            publicationAttempt = -1;
          } else {
            publicationRetries.inc();
            LOG.warn(
                "Error sending message on topic {}, retry attempt {}",
                destinationToSendTo,
                publicationAttempt,
                exception);
          }
        }
      }
    }

    private void outputFailedMessage(
        T input, ProcessContext context, Destination destinationToSendTo, Exception exception) {
      LOG.error("The message wasn't published to topic {}", destinationToSendTo, exception);
      context.output(failedMessagesTag, input);
    }

    private void restartJmsConnection() throws JMSException {
      teardown();
      start();
    }

    @Teardown
    public void teardown() throws JMSException {
      if (producer != null) {
        producer.close();
        producer = null;
      }
      if (session != null) {
        session.close();
        session = null;
      }
      if (connection != null) {
        try {
          // If the connection failed, stopping the connection will throw a JMSException
          connection.stop();
        } catch (JMSException exception) {
          LOG.warn("The connection couldn't be closed", exception);
        }
        connection.close();
        connection = null;
      }
    }
  }
}
