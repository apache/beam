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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.Serializer;
import org.apache.beam.sdk.values.PCollection;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests SerializableCoder.
 */
@RunWith(JUnit4.class)
public class SerializableCoderTest implements Serializable {

  @DefaultCoder(SerializableCoder.class)
  static class MyRecord implements Serializable {
    private static final long serialVersionUID = 42L;

    public String value;

    public MyRecord(String value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
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

  static class StringToRecord extends OldDoFn<String, MyRecord> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(new MyRecord(c.element()));
    }
  }

  static class RecordToString extends OldDoFn<MyRecord, String> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element().value);
    }
  }

  static final List<String> LINES = Arrays.asList(
      "To be,",
      "or not to be");

  @Test
  public void testSerializableCoder() throws Exception {
    IterableCoder<MyRecord> coder = IterableCoder
        .of(SerializableCoder.of(MyRecord.class));

    List<MyRecord> records = new LinkedList<>();
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

    CloudObject encoding = coder.asCloudObject();
    Assert.assertThat(encoding.getClassName(),
        Matchers.containsString(SerializableCoder.class.getSimpleName()));

    Coder<?> decoded = Serializer.deserialize(encoding, Coder.class);
    Assert.assertThat(decoded, Matchers.instanceOf(SerializableCoder.class));
  }

  @Test
  public void testNullEquals() {
    SerializableCoder<MyRecord> coder = SerializableCoder.of(MyRecord.class);
    Assert.assertFalse(coder.equals(null));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDefaultCoder() throws Exception {
    Pipeline p = TestPipeline.create();

    // Use MyRecord as input and output types without explicitly specifying
    // a coder (this uses the default coders, which may not be
    // SerializableCoder).
    PCollection<String> output =
        p.apply(Create.of("Hello", "World"))
        .apply(ParDo.of(new StringToRecord()))
        .apply(ParDo.of(new RecordToString()));

    PAssert.that(output)
        .containsInAnyOrder("Hello", "World");

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
    assertEquals(source, CoderUtils.decodeFromByteArray(coder,
        CoderUtils.encodeToByteArray(coder, source)));

    // Second string uses a UTF8 character.  Each codepoint is translated into
    // 4 characters in UTF8.
    int[] codePoints = new int[20 * 1024];
    Arrays.fill(codePoints, 0x1D50A);  // "MATHEMATICAL_FRAKTUR_CAPITAL_G"
    String source2 = new String(codePoints, 0, codePoints.length);

    // Verify OUTER encoding.
    assertEquals(source2, CoderUtils.decodeFromByteArray(coder,
        CoderUtils.encodeToByteArray(coder, source2)));


    // Encode both strings into NESTED form.
    byte[] nestedEncoding;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      coder.encode(source, os, Coder.Context.NESTED);
      coder.encode(source2, os, Coder.Context.NESTED);
      nestedEncoding = os.toByteArray();
    }

    // Decode from NESTED form.
    try (ByteArrayInputStream is = new ByteArrayInputStream(nestedEncoding)) {
      assertEquals(source, coder.decode(is, Coder.Context.NESTED));
      assertEquals(source2, coder.decode(is, Coder.Context.NESTED));
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
      coder.encode(null, os, Coder.Context.NESTED);
      coder.encode("TestValue", os, Coder.Context.NESTED);
      coder.encode(null, os, Coder.Context.NESTED);
      coder.encode("TestValue2", os, Coder.Context.NESTED);
      coder.encode(null, os, Coder.Context.NESTED);
      encodedBytes = os.toByteArray();
    }

    try (ByteArrayInputStream is = new ByteArrayInputStream(encodedBytes)) {
      assertNull(coder.decode(is, Coder.Context.NESTED));
      assertEquals("TestValue", coder.decode(is,  Coder.Context.NESTED));
      assertNull(coder.decode(is, Coder.Context.NESTED));
      assertEquals("TestValue2", coder.decode(is,  Coder.Context.NESTED));
      assertNull(coder.decode(is, Coder.Context.NESTED));
      assertEquals(0, is.available());
    }
  }

  @Test
  public void testPojoEncodingId() throws Exception {
    Coder<MyRecord> coder = SerializableCoder.of(MyRecord.class);
    CoderProperties.coderHasEncodingId(
        coder,
        String.format("%s:%s", MyRecord.class.getName(), MyRecord.serialVersionUID));
  }
}
