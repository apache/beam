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

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSendMultipleEntry;
import com.solacesystems.jcsmp.XMLMessageProducer;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.RetryCallableManager;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

@Internal
public class SolaceMessageProducer extends MessageProducer {

  private final XMLMessageProducer producer;
  private final RetryCallableManager retryCallableManager = RetryCallableManager.create();

  public SolaceMessageProducer(XMLMessageProducer producer) {
    this.producer = producer;
  }

  @Override
  public void publishSingleMessage(
      Solace.Record record,
      Destination topicOrQueue,
      boolean useCorrelationKeyLatency,
      DeliveryMode deliveryMode) {
    BytesXMLMessage msg = createBytesXMLMessage(record, useCorrelationKeyLatency, deliveryMode);
    Callable<Integer> publish =
        () -> {
          producer.send(msg, topicOrQueue);
          return 0;
        };

    retryCallableManager.retryCallable(publish, ImmutableSet.of(JCSMPException.class));
  }

  @Override
  public int publishBatch(
      List<Solace.Record> records,
      boolean useCorrelationKeyLatency,
      SerializableFunction<Solace.Record, Destination> destinationFn,
      DeliveryMode deliveryMode) {
    JCSMPSendMultipleEntry[] batch =
        createJCSMPSendMultipleEntry(
            records, useCorrelationKeyLatency, destinationFn, deliveryMode);
    Callable<Integer> publish = () -> producer.sendMultiple(batch, 0, batch.length, 0);
    return retryCallableManager.retryCallable(publish, ImmutableSet.of(JCSMPException.class));
  }

  @Override
  public boolean isClosed() {
    return producer == null || producer.isClosed();
  }

  @Override
  public void close() {
    if (!isClosed()) {
      this.producer.close();
    }
  }
}
