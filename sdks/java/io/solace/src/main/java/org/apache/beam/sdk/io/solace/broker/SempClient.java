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

import com.solacesystems.jcsmp.Queue;
import java.io.IOException;
import java.io.Serializable;

/**
 * This interface defines methods for interacting with a Solace message broker using the Solace
 * Element Management Protocol (SEMP). SEMP provides a way to manage and monitor various aspects of
 * the broker, including queues and topics.
 */
public interface SempClient extends Serializable {

  /**
   * Determines if the specified queue is non-exclusive. In Solace, non-exclusive queues allow
   * multiple consumers to receive messages from the queue.
   */
  boolean isQueueNonExclusive(String queueName) throws IOException;

  /**
   * This is only called when a user requests to read data from a topic. This method creates a new
   * queue on the Solace broker and associates it with the specified topic. This ensures that
   * messages published to the topic are delivered to the queue, allowing consumers to receive them.
   */
  Queue createQueueForTopic(String queueName, String topicName) throws IOException;

  /**
   * Retrieves the size of the backlog (in bytes) for the specified queue. The backlog represents
   * the amount of data in messages that are waiting to be delivered to consumers.
   */
  long getBacklogBytes(String queueName) throws IOException;
}
