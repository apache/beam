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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SerializableUtils}. */
@RunWith(JUnit4.class)
public class SerializableUtilsTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();

  /** A class that is serializable by Java. */
  private static class SerializableByJava implements Serializable {
    final String stringValue;
    final int intValue;

    public SerializableByJava(String stringValue, int intValue) {
      this.stringValue = stringValue;
      this.intValue = intValue;
    }
  }

  @Test
  public void testTranscode() {
    String stringValue = "hi bob";
    int intValue = 42;

    SerializableByJava testObject = new SerializableByJava(stringValue, intValue);
    SerializableByJava testCopy = SerializableUtils.ensureSerializable(testObject);

    assertEquals(stringValue, testCopy.stringValue);
    assertEquals(intValue, testCopy.intValue);
  }

  @Test
  public void testDeserializationError() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("unable to deserialize a bogus string");
    SerializableUtils.deserializeFromByteArray(
        "this isn't legal".getBytes(),
        "a bogus string");
  }

  /** A class that is not serializable by Java. */
  private static class UnserializableByJava implements Serializable {
    @SuppressWarnings("unused")
    private Object unserializableField = new Object();
  }

  @Test
  public void testSerializationError() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("unable to serialize");
    SerializableUtils.serializeToByteArray(new UnserializableByJava());
  }

  /** A {@link Coder} that is not serializable by Java. */
  private static class UnserializableCoderByJava extends CustomCoder<Object> {
    private final Object unserializableField = new Object();

    @Override
    public void encode(Object value, OutputStream outStream, Context context)
        throws CoderException, IOException {
    }

    @Override
    public Object decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      return unserializableField;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return ImmutableList.of();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  @Test
  public void testEnsureSerializableWithUnserializableCoderByJava() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("unable to serialize");
    SerializableUtils.ensureSerializable(new UnserializableCoderByJava());
  }
}
