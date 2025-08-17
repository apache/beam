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

import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Base class for publishing messages to a Solace broker.
 *
 * <p>Implementations of this interface are responsible for managing the connection to the broker
 * and for publishing messages to the broker.
 */
@Internal
public interface MessageProducer {

  /** Publishes a message to the broker. */
  void publishSingleMessage(
      Solace.Record msg,
      Destination topicOrQueue,
      boolean useCorrelationKeyLatency,
      DeliveryMode deliveryMode);

  /**
   * Publishes a batch of messages to the broker.
   *
   * <p>The size of the batch cannot exceed 50 messages, this is a limitation of the Solace API.
   *
   * <p>It returns the number of messages written.
   */
  int publishBatch(
      List<Solace.Record> records,
      boolean useCorrelationKeyLatency,
      SerializableFunction<Solace.Record, Destination> destinationFn,
      DeliveryMode deliveryMode);

  /** Returns {@literal true} if the message producer is closed, {@literal false} otherwise. */
  boolean isClosed();

  /** Closes the message producer. */
  void close();
}
