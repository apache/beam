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
 * This interface serves as a blueprint for creating SempClient objects, which are used to interact
 * with a Solace message broker using the Solace Element Management Protocol (SEMP).
 */
public interface SempClientFactory extends Serializable {

  /**
   * This method is the core of the factory interface. It defines how to construct and return a
   * SempClient object. Implementations of this interface will provide the specific logic for
   * creating a client instance, which might involve connecting to the broker, handling
   * authentication, and configuring other settings.
   */
  SempClient create();
}
