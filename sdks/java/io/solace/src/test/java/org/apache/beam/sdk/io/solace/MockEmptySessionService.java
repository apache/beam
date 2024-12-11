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
package org.apache.beam.sdk.io.solace;

import com.google.auto.value.AutoValue;
import com.google.common.util.concurrent.SettableFuture;
import com.solacesystems.jcsmp.JCSMPProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.broker.MessageProducer;
import org.apache.beam.sdk.io.solace.broker.MessageReceiver;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.data.Solace.PublishResult;

@AutoValue
public abstract class MockEmptySessionService extends SessionService {

  String exceptionMessage = "This is an empty client, use a MockSessionService instead.";

  public static MockEmptySessionService create() {
    return new AutoValue_MockEmptySessionService();
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException(exceptionMessage);
  }

  @Override
  public MessageReceiver getReceiver() {
    throw new UnsupportedOperationException(exceptionMessage);
  }

  @Override
  public MessageProducer getInitializedProducer(SubmissionMode mode) {
    throw new UnsupportedOperationException(exceptionMessage);
  }

  @Override
  public ConcurrentHashMap<String, SettableFuture<PublishResult>> getPublishedResultsQueue() {
    throw new UnsupportedOperationException(exceptionMessage);
  }

  @Override
  public void connect() {
    throw new UnsupportedOperationException(exceptionMessage);
  }

  @Override
  public JCSMPProperties initializeSessionProperties(JCSMPProperties baseProperties) {
    throw new UnsupportedOperationException(exceptionMessage);
  }
}
