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
package org.apache.beam.sdk.io.rabbitmq;

import java.io.Serializable;
import java.util.function.Function;
import org.joda.time.Instant;

/**
 * A timestamp policy to assign event time for messages from a RabbitMq queue and watermark for it.
 *
 * @todo use singletons for processing time and timestamp property
 */
@FunctionalInterface
public interface TimestampPolicy extends Function<RabbitMqMessage, Instant>, Serializable {

  static TimestampPolicy processingTime() {
    return new ProcessingTimePolicy();
  }

  static TimestampPolicy timestampProperty() {
    return new TimestampPropertyPolicy();
  }

  class ProcessingTimePolicy implements TimestampPolicy {
    @Override
    public Instant apply(RabbitMqMessage rabbitMqMessage) {
      return Instant.now();
    }
  }

  class TimestampPropertyPolicy implements TimestampPolicy {
    @Override
    public Instant apply(RabbitMqMessage rabbitMqMessage) {
      return new Instant(rabbitMqMessage.getTimestamp());
    }
  }
}
