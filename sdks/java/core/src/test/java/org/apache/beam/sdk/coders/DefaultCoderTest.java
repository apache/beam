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

import static com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests of Coder defaults.
 */
@RunWith(JUnit4.class)
public class DefaultCoderTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  public CoderRegistry registry = new CoderRegistry();

  @Before
  public void registerStandardCoders() {
    registry.registerStandardCoders();
  }

  @DefaultCoder(AvroCoder.class)
  private static class AvroRecord {
  }

  private static class SerializableBase implements Serializable {
  }

  @DefaultCoder(SerializableCoder.class)
  private static class SerializableRecord extends SerializableBase {
  }

  @DefaultCoder(CustomSerializableCoder.class)
  private static class CustomRecord extends SerializableBase {
  }

  @DefaultCoder(OldCustomSerializableCoder.class)
  private static class OldCustomRecord extends SerializableBase {
  }

  private static class Unknown {
  }

  private static class CustomSerializableCoder extends SerializableCoder<CustomRecord> {
    // Extending SerializableCoder isn't trivial, but it can be done.
    @SuppressWarnings("unchecked")
    public static <T extends Serializable> SerializableCoder<T> of(TypeDescriptor<T> recordType) {
       checkArgument(recordType.isSupertypeOf(new TypeDescriptor<CustomRecord>() {}));
       return (SerializableCoder<T>) new CustomSerializableCoder();
    }

    protected CustomSerializableCoder() {
      super(CustomRecord.class);
    }
  }

  private static class OldCustomSerializableCoder extends SerializableCoder<OldCustomRecord> {
    // Extending SerializableCoder isn't trivial, but it can be done.
    @Deprecated // old form using a Class
    @SuppressWarnings("unchecked")
    public static <T extends Serializable> SerializableCoder<T> of(Class<T> recordType) {
       checkArgument(OldCustomRecord.class.isAssignableFrom(recordType));
       return (SerializableCoder<T>) new OldCustomSerializableCoder();
    }

    protected OldCustomSerializableCoder() {
      super(OldCustomRecord.class);
    }
  }

  @Test
  public void testDefaultCoderClasses() throws Exception {
    assertThat(registry.getDefaultCoder(AvroRecord.class), instanceOf(AvroCoder.class));
    assertThat(registry.getDefaultCoder(SerializableBase.class),
        instanceOf(SerializableCoder.class));
    assertThat(registry.getDefaultCoder(SerializableRecord.class),
        instanceOf(SerializableCoder.class));
    assertThat(registry.getDefaultCoder(CustomRecord.class),
        instanceOf(CustomSerializableCoder.class));
    assertThat(registry.getDefaultCoder(OldCustomRecord.class),
        instanceOf(OldCustomSerializableCoder.class));
  }

  @Test
  public void testDefaultCoderInCollection() throws Exception {
    assertThat(registry.getDefaultCoder(new TypeDescriptor<List<AvroRecord>>(){}),
        instanceOf(ListCoder.class));
    assertThat(registry.getDefaultCoder(new TypeDescriptor<List<SerializableRecord>>(){}),
        equalTo((Coder<List<SerializableRecord>>)
            ListCoder.of(SerializableCoder.of(SerializableRecord.class))));
  }

  @Test
  public void testUnknown() throws Exception {
    thrown.expect(CannotProvideCoderException.class);
    registry.getDefaultCoder(Unknown.class);
  }
}
