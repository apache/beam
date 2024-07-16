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
package org.apache.beam.sdk.io.solace.write.properties;

import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;

public class MockSessionPropertiesProvider extends SessionPropertiesProvider {
  private final SubmissionMode mode;

  public MockSessionPropertiesProvider(SubmissionMode mode) {
    this.mode = mode;
  }

  @Override
  public JCSMPProperties initializeSessionProperties(JCSMPProperties baseProperties) {
    // Let's override some properties that will be overriden by the connector
    // Opposite of the mode, to test that is overriden
    baseProperties.setProperty(
        JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR, mode == SubmissionMode.HIGHER_THROUGHPUT);

    baseProperties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, 87);

    return baseProperties;
  }
}
