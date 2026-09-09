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
package org.apache.beam.sdk.extensions.openlineage;

import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportBuilder;
import io.openlineage.client.transports.TransportConfig;

/** Registers {@link RejectingTransport} with the OpenLineage client's transport resolver. */
public class RejectingTransportBuilder implements TransportBuilder {

  @Override
  public String getType() {
    return "beamTestRejecting";
  }

  @Override
  public TransportConfig getConfig() {
    return new RejectingTransportConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    return new RejectingTransport((RejectingTransportConfig) config);
  }
}
