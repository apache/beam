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
package org.apache.beam.sdk.io.kafka.serialization;

import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.joda.time.Instant;

/**
 * Kafka {@link Deserializer} for {@link Instant}.
 *
 * <p>This decodes the number of milliseconds since epoch using {@link LongDeserializer}.
 */
@Experimental(Kind.SOURCE_SINK)
public class InstantDeserializer implements Deserializer<Instant> {
  private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public Instant deserialize(String topic, byte[] bytes) {
    return new Instant(LONG_DESERIALIZER.deserialize(topic, bytes));
  }

  @Override
  public void close() {}
}
