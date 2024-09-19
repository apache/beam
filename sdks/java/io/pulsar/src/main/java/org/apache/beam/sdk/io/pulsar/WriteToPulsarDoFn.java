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

/**
 * Transform for writing to Apache Pulsar. Support is currently incomplete, and there may be bugs;
 * see https://github.com/apache/beam/issues/31078 for more info, and comment in that issue if you
 * run into issues with this IO.
 */
@DoFn.UnboundedPerElement
@SuppressWarnings({"rawtypes", "nullness"})
public class WriteToPulsarDoFn extends DoFn<byte[], Void> {

  private Producer<byte[]> producer;
  private PulsarClient client;
  private String clientUrl;
  private String topic;

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
    producer.send(messageToSend);
  }

  @Teardown
  public void teardown() throws PulsarClientException {
    producer.close();
    client.close();
  }
}
