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
package org.apache.beam.sdk.io.solace;

import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import java.time.Instant;
import java.util.List;
import org.apache.beam.sdk.io.solace.broker.MessageProducer;
import org.apache.beam.sdk.io.solace.broker.PublishResultHandler;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.transforms.SerializableFunction;

public abstract class MockProducer implements MessageProducer {
  final PublishResultHandler handler;

  public MockProducer(PublishResultHandler handler) {
    this.handler = handler;
  }

  @Override
  public int publishBatch(
      List<Record> records,
      boolean useCorrelationKeyLatency,
      SerializableFunction<Record, Destination> destinationFn,
      DeliveryMode deliveryMode) {
    for (Record record : records) {
      this.publishSingleMessage(
          record, destinationFn.apply(record), useCorrelationKeyLatency, deliveryMode);
    }
    return records.size();
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void close() {}

  public static class MockSuccessProducer extends MockProducer {
    public MockSuccessProducer(PublishResultHandler handler) {
      super(handler);
    }

    @Override
    public void publishSingleMessage(
        Record msg,
        Destination topicOrQueue,
        boolean useCorrelationKeyLatency,
        DeliveryMode deliveryMode) {
      if (useCorrelationKeyLatency) {
        handler.responseReceivedEx(
            Solace.PublishResult.builder()
                .setPublished(true)
                .setMessageId(msg.getMessageId())
                .build());
      } else {
        handler.responseReceivedEx(msg.getMessageId());
      }
    }
  }

  public static class MockFailedProducer extends MockProducer {
    public MockFailedProducer(PublishResultHandler handler) {
      super(handler);
    }

    @Override
    public void publishSingleMessage(
        Record msg,
        Destination topicOrQueue,
        boolean useCorrelationKeyLatency,
        DeliveryMode deliveryMode) {
      if (useCorrelationKeyLatency) {
        handler.handleErrorEx(
            Solace.PublishResult.builder()
                .setPublished(false)
                .setMessageId(msg.getMessageId())
                .setError("Some error")
                .build(),
            new JCSMPException("Some JCSMPException"),
            Instant.now().toEpochMilli());
      } else {
        handler.handleErrorEx(
            msg.getMessageId(),
            new JCSMPException("Some JCSMPException"),
            Instant.now().toEpochMilli());
      }
    }
  }
}
