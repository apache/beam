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
package org.apache.beam.sdk.io.jms;

import java.io.Serializable;
import javax.jms.ConnectionFactory;

/**
 * An interface for creating custom JMS {@link ConnectionFactory} instances.
 *
 * <p>Expansion service users connecting to JMS brokers other than the built-in supported ones
 * (ActiveMQ, Qpid, IBM MQ) can implement this interface and specify their implementation class name
 * in {@link ConnectionConfiguration}.
 */
@FunctionalInterface
public interface BeamGenericJmsConnectionFactory extends Serializable {

  /**
   * Creates a {@link ConnectionFactory} using the given {@link ConnectionConfiguration}.
   *
   * @param config the JMS connection configuration
   * @return configured JMS {@link ConnectionFactory}
   */
  ConnectionFactory createConnectionFactory(ConnectionConfiguration config) throws Exception;
}
