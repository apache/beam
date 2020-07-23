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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.stubbing.Answer;

/** Tests SerializableCoder. */
@RunWith(JUnit4.class)
public class SerializableCoderTest implements Serializable {
  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(SerializableCoder.class);

  @DefaultCoder(SerializableCoder.class)
  static class MyRecord implements Serializable {
    private static final long serialVersionUID = 42L;

    public String value;

    public MyRecord(String value) {
      this.value = value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      MyRecord myRecord = (MyRecord) o;
      return value.equals(myRecord.value);
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }

  static class StringToRecord extends DoFn<String, MyRecord> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new MyRecord(c.element()));
    }
  }

  static class RecordToString extends DoFn<MyRecord, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().value);
    }
  }

  static final List<String> LINES = Arrays.asList("To be,", "or not to be");

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testSerializableCoder() throws Exception {
    IterableCoder<MyRecord> coder = IterableCoder.of(SerializableCoder.of(MyRecord.class));

    List<MyRecord> records = new ArrayList<>();
    for (String l : LINES) {
      records.add(new MyRecord(l));
    }

    byte[] encoded = CoderUtils.encodeToByteArray(coder, records);
    Iterable<MyRecord> decoded = CoderUtils.decodeFromByteArray(coder, encoded);

    assertEquals(records, decoded);
  }

  @Test
  public void testSerializableCoderConstruction() throws Exception {
    SerializableCoder<MyRecord> coder = SerializableCoder.of(MyRecord.class);
    assertEquals(coder.getRecordType(), MyRecord.class);
    CoderProperties.coderSerializable(coder);

    SerializableCoder<?> decoded = SerializableUtils.clone(coder);
    assertThat(decoded.getRecordType(), Matchers.<Object>equalTo(MyRecord.class));
  }

  @Test
  public <T extends Serializable> void testSerializableCoderIsSerializableWithGenericTypeToken()
      throws Exception {
    SerializableCoder<T> coder = SerializableCoder.of(new TypeDescriptor<T>() {});
    CoderProperties.coderSerializable(coder);
  }

  @Test
  public void testNullEquals() {
    SerializableCoder<MyRecord> coder = SerializableCoder.of(MyRecord.class);
    Assert.assertFalse(coder.equals(null));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDefaultCoder() throws Exception {
    p.enableAbandonedNodeEnforcement(true);

    // Use MyRecord as input and output types without explicitly specifying
    // a coder (this uses the default coders, which may not be
    // SerializableCoder).
    PCollection<String> output =
        p.apply(Create.of("Hello", "World"))
            .apply(ParDo.of(new StringToRecord()))
            .apply(ParDo.of(new RecordToString()));

    PAssert.that(output).containsInAnyOrder("Hello", "World");

    p.run();
  }

  @Test
  public void testLongStringEncoding() throws Exception {
    StringUtf8Coder coder = StringUtf8Coder.of();

    // Java's DataOutputStream.writeUTF fails at 64k, so test well beyond that.
    char[] chars = new char[100 * 1024];
    Arrays.fill(chars, 'o');
    String source = new String(chars);

    // Verify OUTER encoding.
    assertEquals(
        source, CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, source)));

    // Second string uses a UTF8 character.  Each codepoint is translated into
    // 4 characters in UTF8.
    int[] codePoints = new int[20 * 1024];
    Arrays.fill(codePoints, 0x1D50A); // "MATHEMATICAL_FRAKTUR_CAPITAL_G"
    String source2 = new String(codePoints, 0, codePoints.length);

    // Verify OUTER encoding.
    assertEquals(
        source2,
        CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, source2)));

    // Encode both strings into NESTED form.
    byte[] nestedEncoding;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      coder.encode(source, os);
      coder.encode(source2, os);
      nestedEncoding = os.toByteArray();
    }

    // Decode from NESTED form.
    try (ByteArrayInputStream is = new ByteArrayInputStream(nestedEncoding)) {
      assertEquals(source, coder.decode(is));
      assertEquals(source2, coder.decode(is));
      assertEquals(0, is.available());
    }
  }

  @Test
  public void testNullEncoding() throws Exception {
    Coder<String> coder = SerializableCoder.of(String.class);
    byte[] encodedBytes = CoderUtils.encodeToByteArray(coder, null);
    assertNull(CoderUtils.decodeFromByteArray(coder, encodedBytes));
  }

  @Test
  public void testMixedWithNullsEncoding() throws Exception {
    Coder<String> coder = SerializableCoder.of(String.class);
    byte[] encodedBytes;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      coder.encode(null, os);
      coder.encode("TestValue", os);
      coder.encode(null, os);
      coder.encode("TestValue2", os);
      coder.encode(null, os);
      encodedBytes = os.toByteArray();
    }

    try (ByteArrayInputStream is = new ByteArrayInputStream(encodedBytes)) {
      assertNull(coder.decode(is));
      assertEquals("TestValue", coder.decode(is));
      assertNull(coder.decode(is));
      assertEquals("TestValue2", coder.decode(is));
      assertNull(coder.decode(is));
      assertEquals(0, is.available());
    }
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    assertThat(
        SerializableCoder.of(MyRecord.class).getEncodedTypeDescriptor(),
        Matchers.equalTo(TypeDescriptor.of(MyRecord.class)));
  }

  private static class AutoRegistration implements Serializable {
    private static final long serialVersionUID = 42L;
  }

  @Test
  public void testSerializableCoderProviderIsRegistered() throws Exception {
    assertThat(
        CoderRegistry.createDefault().getCoder(AutoRegistration.class),
        instanceOf(SerializableCoder.class));
  }

  private interface TestInterface extends Serializable {}

  @Test
  public void coderWarnsForInterface() throws Exception {
    String expectedLogMessage =
        "Can't verify serialized elements of type TestInterface "
            + "have well defined equals method.";
    // Create the coder multiple times ensuring that we only log once.
    SerializableCoder.of(TestInterface.class);
    SerializableCoder.of(TestInterface.class);
    SerializableCoder.of(TestInterface.class);
    expectedLogs.verifyLogRecords(
        new TypeSafeMatcher<Iterable<LogRecord>>() {
          @Override
          public void describeTo(Description description) {
            description.appendText(
                String.format("single warn log message containing [%s]", expectedLogMessage));
          }

          @Override
          protected boolean matchesSafely(Iterable<LogRecord> item) {
            int count = 0;
            for (LogRecord logRecord : item) {
              if (logRecord.getLevel().equals(Level.WARNING)
                  && logRecord.getMessage().contains(expectedLogMessage)) {
                count += 1;
              }
            }
            return count == 1;
          }
        });
  }

  private static class NoEquals implements Serializable {}

  @Test
  public void coderWarnsForNoEquals() throws Exception {
    String expectedLogMessage =
        "Can't verify serialized elements of type NoEquals " + "have well defined equals method.";
    // Create the coder multiple times ensuring that we only log once.
    SerializableCoder.of(NoEquals.class);
    SerializableCoder.of(NoEquals.class);
    SerializableCoder.of(NoEquals.class);
    expectedLogs.verifyLogRecords(
        new TypeSafeMatcher<Iterable<LogRecord>>() {
          @Override
          public void describeTo(Description description) {
            description.appendText(
                String.format("single warn log message containing [%s]", expectedLogMessage));
          }

          @Override
          protected boolean matchesSafely(Iterable<LogRecord> item) {
            int count = 0;
            for (LogRecord logRecord : item) {
              if (logRecord.getLevel().equals(Level.WARNING)
                  && logRecord.getMessage().contains(expectedLogMessage)) {
                count += 1;
              }
            }
            return count == 1;
          }
        });
  }

  private static class ProperEquals implements Serializable {
    private int x;

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ProperEquals that = (ProperEquals) o;

      return x == that.x;
    }

    @Override
    public int hashCode() {
      return x;
    }
  }

  @Test
  public void coderChecksForEquals() throws Exception {
    SerializableCoder.of(ProperEquals.class);
    expectedLogs.verifyNotLogged("Can't verify serialized elements of type");
  }

  @Test(expected = IOException.class)
  public void coderDoesNotWrapIoException() throws Exception {
    final SerializableCoder<String> coder = SerializableCoder.of(String.class);

    final OutputStream outputStream =
        mock(
            OutputStream.class,
            (Answer)
                invocationOnMock -> {
                  throw new IOException();
                });

    coder.encode("", outputStream);
  }
}
