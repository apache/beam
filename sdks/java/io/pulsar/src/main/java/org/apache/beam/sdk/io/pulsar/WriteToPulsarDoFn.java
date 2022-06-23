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
package org.apache.beam.sdk.io.pulsar;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@DoFn.UnboundedPerElement
@SuppressWarnings({"rawtypes", "nullness"})
public class WriteToPulsarDoFn extends DoFn<byte[], Void> {

  private static final Logger LOG = LoggerFactory.getLogger(WriteToPulsarDoFn.class);

  private Producer<byte[]> producer;
  private PulsarClient client;
  private String clientUrl;
  private String topic;

  private transient Exception sendException = null;
  private transient long numSendFailures = 0;

  WriteToPulsarDoFn(PulsarIO.Write transform) {
    this.clientUrl = transform.getClientUrl();
    this.topic = transform.getTopic();
  }

  @Setup
  public void setup() throws PulsarClientException {
    client = PulsarClient.builder().serviceUrl(clientUrl).build();
    producer = client.newProducer().topic(topic).compressionType(CompressionType.LZ4).create();
  }

  @ProcessElement
  public void processElement(@Element byte[] messageToSend) throws Exception {
    producer.sendAsync(messageToSend)
            .whenComplete((mid, exception) -> {
              if (exception == null) {
                return;
              }

              synchronized (WriteToPulsarDoFn.this) {
                if (sendException == null) {
                  sendException = (Exception) exception;
                }
                numSendFailures++;
              }
              // don't log exception stacktrace here, exception will be propagated up.
              LOG.warn("send failed : '{}'", exception.getMessage());
            });
  }

  @FinishBundle
  public void finishBundle() throws IOException {
    producer.flush();
    checkForFailures();
  }

  @Teardown
  public void teardown() throws PulsarClientException {
    producer.close();
    client.close();
  }

  private synchronized void checkForFailures() throws IOException {
    if (numSendFailures == 0) {
      return;
    }

    String msg = String.format(
            "Pulsar Write DoFn: failed to send %d records (since last report)", numSendFailures);

    Exception e = sendException;
    sendException = null;
    numSendFailures = 0;

    LOG.warn(msg);
    throw new IOException(msg, e);
  }
}
