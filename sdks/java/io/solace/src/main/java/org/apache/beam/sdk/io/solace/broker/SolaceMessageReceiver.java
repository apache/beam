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
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.StaleSessionException;
import java.io.IOException;
import org.apache.beam.sdk.io.solace.RetryCallableManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

public class SolaceMessageReceiver implements MessageReceiver {
  public static final int DEFAULT_ADVANCE_TIMEOUT_IN_MILLIS = 100;
  private final FlowReceiver flowReceiver;
  private final RetryCallableManager retryCallableManager = RetryCallableManager.create();

  public SolaceMessageReceiver(FlowReceiver flowReceiver) {
    this.flowReceiver = flowReceiver;
  }

  @Override
  public void start() {
    startFlowReceiver();
  }

  private void startFlowReceiver() {
    retryCallableManager.retryCallable(
        () -> {
          flowReceiver.start();
          return 0;
        },
        ImmutableSet.of(JCSMPException.class));
  }

  @Override
  public BytesXMLMessage receive() throws IOException {
    try {
      return flowReceiver.receive(DEFAULT_ADVANCE_TIMEOUT_IN_MILLIS);
    } catch (StaleSessionException e) {
      startFlowReceiver();
      throw new IOException(
          "SolaceIO: Caught StaleSessionException, restarting the FlowReceiver.", e);
    } catch (JCSMPException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() {
    flowReceiver.close();
  }
}
