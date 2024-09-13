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
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSendMultipleEntry;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.transforms.SerializableFunction;

@Internal
public class MessageProducerUtils {
  // This is the batch limit supported by the send multiple JCSMP API method.
  static final int SOLACE_BATCH_LIMIT = 50;

  /**
   * Create a {@link BytesXMLMessage} to be published in Solace.
   *
   * @param record The record to be published.
   * @param useCorrelationKeyLatency Whether to use a complex key for tracking latency.
   * @param deliveryMode The {@link DeliveryMode} used to publish the message.
   * @return A {@link BytesXMLMessage} that can be sent to Solace "as is".
   */
  public static BytesXMLMessage createBytesXMLMessage(
      Solace.Record record, boolean useCorrelationKeyLatency, DeliveryMode deliveryMode) {
    JCSMPFactory jcsmpFactory = JCSMPFactory.onlyInstance();
    BytesXMLMessage msg = jcsmpFactory.createBytesXMLMessage();
    byte[] payload = record.getPayload();
    msg.writeBytes(payload);

    Long senderTimestamp = record.getSenderTimestamp();
    if (senderTimestamp == null) {
      senderTimestamp = System.currentTimeMillis();
    }
    msg.setSenderTimestamp(senderTimestamp);
    msg.setDeliveryMode(deliveryMode);
    if (useCorrelationKeyLatency) {
      Solace.CorrelationKey key =
          Solace.CorrelationKey.builder()
              .setMessageId(record.getMessageId())
              .setPublishMonotonicMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()))
              .build();
      msg.setCorrelationKey(key);
    } else {
      // Use only a string as correlation key
      msg.setCorrelationKey(record.getMessageId());
    }
    msg.setApplicationMessageId(record.getMessageId());
    return msg;
  }

  /**
   * Create a {@link JCSMPSendMultipleEntry} array to be published in Solace. This can be used with
   * `sendMultiple` to send all the messages in a single API call.
   *
   * <p>The size of the list cannot be larger than 50 messages. This is a hard limit enforced by the
   * Solace API.
   *
   * @param records A {@link List} of records to be published
   * @param useCorrelationKeyLatency Whether to use a complex key for tracking latency.
   * @param destinationFn A function that maps every record to its destination.
   * @param deliveryMode The {@link DeliveryMode} used to publish the message.
   * @return A {@link JCSMPSendMultipleEntry} array that can be sent to Solace "as is".
   */
  public static JCSMPSendMultipleEntry[] createJCSMPSendMultipleEntry(
      List<Solace.Record> records,
      boolean useCorrelationKeyLatency,
      SerializableFunction<Solace.Record, Destination> destinationFn,
      DeliveryMode deliveryMode) {
    if (records.size() > SOLACE_BATCH_LIMIT) {
      throw new RuntimeException(
          String.format(
              "SolaceIO.Write: Trying to create a batch of %d, but Solace supports a"
                  + " maximum of %d. The batch will likely be rejected by Solace.",
              records.size(), SOLACE_BATCH_LIMIT));
    }

    JCSMPSendMultipleEntry[] entries = new JCSMPSendMultipleEntry[records.size()];
    for (int i = 0; i < records.size(); i++) {
      Solace.Record record = records.get(i);
      JCSMPSendMultipleEntry entry =
          JCSMPFactory.onlyInstance()
              .createSendMultipleEntry(
                  createBytesXMLMessage(record, useCorrelationKeyLatency, deliveryMode),
                  destinationFn.apply(record));
      entries[i] = entry;
    }

    return entries;
  }
}
