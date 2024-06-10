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

import java.io.Serializable;

/**
 * The SessionService interface provides a set of methods for managing a session with the Solace
 * messaging system. It allows for establishing a connection, creating a message-receiver object,
 * checking if the connection is closed or not, and gracefully closing the session.
 */
public interface SessionService extends Serializable {

  /**
   * Establishes a connection to the service. This could involve providing connection details like
   * host, port, VPN name, username, and password.
   */
  void connect();

  /** Gracefully closes the connection to the service. */
  void close();

  /**
   * Checks whether the connection to the service is currently closed. This method is called when an
   * `UnboundedSolaceReader` is starting to read messages - a session will be created if this
   * returns true.
   */
  boolean isClosed();

  /**
   * Creates a MessageReceiver object for receiving messages from Solace. Typically, this object is
   * created from the session instance.
   */
  MessageReceiver createReceiver();
}
