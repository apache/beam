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
package org.apache.beam.sdk.coders;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.DefaultCoder.DefaultCoderProviderRegistrar.DefaultCoderProvider;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DefaultCoder}. */
@RunWith(JUnit4.class)
public class DefaultCoderTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @DefaultCoder(MockDefaultCoder.class)
  private static class AvroRecord {}

  private static class SerializableBase implements Serializable {}

  @DefaultCoder(SerializableCoder.class)
  private static class SerializableRecord extends SerializableBase {}

  @DefaultCoder(CustomSerializableCoder.class)
  private static class CustomRecord extends SerializableBase {}

  @DefaultCoder(OldCustomSerializableCoder.class)
  private static class OldCustomRecord extends SerializableBase {}

  private static class Unknown {}

  private static class CustomSerializableCoder extends SerializableCoder<CustomRecord> {
    // Extending SerializableCoder isn't trivial, but it can be done.
    @SuppressWarnings("unchecked")
    public static <T extends Serializable> SerializableCoder<T> of(TypeDescriptor<T> recordType) {
      checkArgument(recordType.isSupertypeOf(new TypeDescriptor<CustomRecord>() {}));
      return (SerializableCoder<T>) new CustomSerializableCoder();
    }

    protected CustomSerializableCoder() {
      super(CustomRecord.class, TypeDescriptor.of(CustomRecord.class));
    }

    @SuppressWarnings("unused")
    public static CoderProvider getCoderProvider() {
      return new CoderProvider() {
        @Override
        public <T> Coder<T> coderFor(
            TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
            throws CannotProvideCoderException {
          return CustomSerializableCoder.of((TypeDescriptor) typeDescriptor);
        }
      };
    }
  }

  private static class OldCustomSerializableCoder extends SerializableCoder<OldCustomRecord> {
    // Extending SerializableCoder isn't trivial, but it can be done.

    // Old form using a Class.
    @SuppressWarnings("unchecked")
    public static <T extends Serializable> SerializableCoder<T> of(Class<T> recordType) {
      checkArgument(OldCustomRecord.class.isAssignableFrom(recordType));
      return (SerializableCoder<T>) new OldCustomSerializableCoder();
    }

    protected OldCustomSerializableCoder() {
      super(OldCustomRecord.class, TypeDescriptor.of(OldCustomRecord.class));
    }

    @SuppressWarnings("unused")
    public static CoderProvider getCoderProvider() {
      return new CoderProvider() {
        @Override
        public <T> Coder<T> coderFor(
            TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
            throws CannotProvideCoderException {
          return OldCustomSerializableCoder.of((Class) typeDescriptor.getRawType());
        }
      };
    }
  }

  @Test
  public void testCodersWithoutComponents() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault(null);
    registry.registerCoderProvider(new DefaultCoderProvider());
    assertThat(registry.getCoder(AvroRecord.class), instanceOf(MockDefaultCoder.class));
    assertThat(registry.getCoder(SerializableRecord.class), instanceOf(SerializableCoder.class));
    assertThat(registry.getCoder(CustomRecord.class), instanceOf(CustomSerializableCoder.class));
    assertThat(
        registry.getCoder(OldCustomRecord.class), instanceOf(OldCustomSerializableCoder.class));
  }

  @Test
  public void testDefaultCoderInCollection() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault(null);
    registry.registerCoderProvider(new DefaultCoderProvider());
    Coder<List<AvroRecord>> avroRecordCoder =
        registry.getCoder(new TypeDescriptor<List<AvroRecord>>() {});
    assertThat(avroRecordCoder, instanceOf(ListCoder.class));
    assertThat(((ListCoder) avroRecordCoder).getElemCoder(), instanceOf(MockDefaultCoder.class));
    assertThat(
        registry.getCoder(new TypeDescriptor<List<SerializableRecord>>() {}),
        Matchers.equalTo(ListCoder.of(SerializableCoder.of(SerializableRecord.class))));
  }

  @Test
  public void testUnknown() throws Exception {
    thrown.expect(CannotProvideCoderException.class);
    new DefaultCoderProvider().coderFor(TypeDescriptor.of(Unknown.class), Collections.emptyList());
  }
}
