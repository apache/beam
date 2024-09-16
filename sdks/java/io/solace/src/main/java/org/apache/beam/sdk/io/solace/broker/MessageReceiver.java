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
import java.io.IOException;

/**
 * Interface for receiving messages from a Solace broker.
 *
 * <p>Implementations of this interface are responsible for managing the connection to the broker
 * and for receiving messages from the broker.
 */
public interface MessageReceiver {
  /**
   * Starts the message receiver.
   *
   * <p>This method is called in the {@link
   * org.apache.beam.sdk.io.solace.read.UnboundedSolaceReader#start()} method.
   */
  void start();

  /**
   * Returns {@literal true} if the message receiver is closed, {@literal false} otherwise.
   *
   * <p>A message receiver is closed when it is no longer able to receive messages.
   */
  boolean isClosed();

  /**
   * Receives a message from the broker.
   *
   * <p>This method will block until a message is received.
   */
  BytesXMLMessage receive() throws IOException;

  /** Closes the message receiver. */
  void close();

  /**
   * Test clients may return {@literal true} to signal that all expected messages have been pulled
   * and the test may complete. Real clients should always return {@literal false}.
   */
  default boolean isEOF() {
    return false;
  }
}
