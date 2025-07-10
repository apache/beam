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
import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.beam.sdk.io.solace.MockProducer.MockFailedProducer;
import org.apache.beam.sdk.io.solace.MockProducer.MockSuccessProducer;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
public abstract class MockSessionServiceFactory extends SessionServiceFactory {
  public abstract @Nullable SubmissionMode mode();

  public abstract @Nullable SerializableFunction<Integer, BytesXMLMessage> recordFn();

  public abstract int minMessagesReceived();

  public abstract SessionServiceType sessionServiceType();

  public static Builder builder() {
    return new AutoValue_MockSessionServiceFactory.Builder()
        .minMessagesReceived(0)
        .sessionServiceType(SessionServiceType.WITH_SUCCEEDING_PRODUCER);
  }

  public static SessionServiceFactory getDefaultMock() {
    return MockSessionServiceFactory.builder().build();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder mode(@Nullable SubmissionMode mode);

    public abstract Builder recordFn(
        @Nullable SerializableFunction<Integer, BytesXMLMessage> recordFn);

    public abstract Builder minMessagesReceived(int minMessagesReceived);

    public abstract Builder sessionServiceType(SessionServiceType sessionServiceType);

    public abstract MockSessionServiceFactory build();
  }

  @Override
  public SessionService create() {
    switch (sessionServiceType()) {
      case EMPTY:
        return MockEmptySessionService.create();
      case WITH_SUCCEEDING_PRODUCER:
        return MockSessionService.builder()
            .recordFn(recordFn())
            .minMessagesReceived(minMessagesReceived())
            .mode(mode())
            .mockProducerFn(MockSuccessProducer::new)
            .build();
      case WITH_FAILING_PRODUCER:
        return MockSessionService.builder()
            .recordFn(recordFn())
            .minMessagesReceived(minMessagesReceived())
            .mode(mode())
            .mockProducerFn(MockFailedProducer::new)
            .build();
      default:
        throw new RuntimeException(
            String.format("Unknown sessionServiceType: %s", sessionServiceType().name()));
    }
  }

  public enum SessionServiceType {
    EMPTY,
    WITH_SUCCEEDING_PRODUCER,
    WITH_FAILING_PRODUCER
  }
}
