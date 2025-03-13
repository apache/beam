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
package org.apache.beam.sdk.io.kafka;

import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.kafka.serialization.InstantDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of {@link LocalDeserializerProvider}. */
@RunWith(JUnit4.class)
public class LocalDeserializerProviderTest {
  @Rule public ExpectedException cannotInferException = ExpectedException.none();

  @Test
  public void testInferKeyCoder() {
    CoderRegistry registry = CoderRegistry.createDefault();
    assertTrue(
        LocalDeserializerProvider.of(LongDeserializer.class)
                .getNullableCoder(registry)
                .getValueCoder()
            instanceof VarLongCoder);
    assertTrue(
        LocalDeserializerProvider.of(StringDeserializer.class)
                .getNullableCoder(registry)
                .getValueCoder()
            instanceof StringUtf8Coder);
    assertTrue(
        LocalDeserializerProvider.of(InstantDeserializer.class)
                .getNullableCoder(registry)
                .getValueCoder()
            instanceof InstantCoder);
    assertTrue(
        LocalDeserializerProvider.of(DeserializerWithInterfaces.class)
                .getNullableCoder(registry)
                .getValueCoder()
            instanceof VarLongCoder);
  }

  @Test
  public void testInferKeyCoderFailure() throws Exception {
    cannotInferException.expect(RuntimeException.class);

    CoderRegistry registry = CoderRegistry.createDefault();
    LocalDeserializerProvider.of(NonInferableObjectDeserializer.class).getCoder(registry);
  }

  // interface for testing coder inference
  private interface DummyInterface<T> {}

  // interface for testing coder inference
  private interface DummyNonparametricInterface {}

  // class for testing coder inference
  private static class DeserializerWithInterfaces
      implements DummyInterface<String>, DummyNonparametricInterface, Deserializer<Long> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Long deserialize(String topic, byte[] bytes) {
      return 0L;
    }

    @Override
    public void close() {}
  }

  // class for which a coder cannot be infered
  private static class NonInferableObject {}

  // class for testing coder inference
  private static class NonInferableObjectDeserializer implements Deserializer<NonInferableObject> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public NonInferableObject deserialize(String topic, byte[] bytes) {
      return new NonInferableObject();
    }

    @Override
    public void close() {}
  }
}
