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
package org.apache.beam.sdk.extensions.kryo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/** Test targeted at {@link KryoCoder}. */
public class KryoCoderTest {

  private static final PipelineOptions OPTIONS = PipelineOptionsFactory.create();

  @Test
  public void testBasicCoding() throws IOException {
    final KryoCoder<ClassToBeEncoded> coder =
        KryoCoder.of(OPTIONS, k -> k.register(ClassToBeEncoded.class));
    assertEncoding(coder);
  }

  @Test(expected = CoderException.class)
  public void testWrongRegistrarCoding() throws IOException {
    OPTIONS.as(KryoOptions.class).setKryoRegistrationRequired(true);
    final KryoCoder<ClassToBeEncoded> coder = KryoCoder.of(OPTIONS);
    assertEncoding(coder);
  }

  @Test(expected = CoderException.class)
  public void testWrongRegistrarDecoding() throws IOException {
    final KryoRegistrar registrarCoding = k -> k.register(ClassToBeEncoded.class);
    final KryoRegistrar registrarDecoding =
        k -> {
          // No-op
        };
    final KryoCoder<ClassToBeEncoded> coderToEncode = KryoCoder.of(OPTIONS, registrarCoding);
    final KryoCoder<ClassToBeEncoded> coderToDecode = KryoCoder.of(OPTIONS, registrarDecoding);
    assertEncoding(coderToEncode, coderToDecode);
  }

  @Test
  public void testCodingOfTwoClassesInSerial() throws IOException {
    final KryoRegistrar registrar =
        k -> {
          k.register(ClassToBeEncoded.class);
          k.register(TestClass.class);
        };
    final KryoCoder<ClassToBeEncoded> coder = KryoCoder.of(OPTIONS, registrar);
    final KryoCoder<TestClass> secondCoder = KryoCoder.of(OPTIONS, registrar);

    final ClassToBeEncoded originalValue = new ClassToBeEncoded("XyZ", 42, Double.NaN);
    final TestClass secondOriginalValue = new TestClass("just a parameter");
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    coder.encode(originalValue, outputStream);
    secondCoder.encode(secondOriginalValue, outputStream);

    final byte[] buf = outputStream.toByteArray();
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);

    final ClassToBeEncoded decodedValue = coder.decode(inputStream);
    final TestClass secondDecodedValue = secondCoder.decode(inputStream);

    assertNotNull(decodedValue);
    assertEquals(originalValue, decodedValue);

    assertNotNull(secondDecodedValue);
    assertNotNull(secondDecodedValue.param);
    assertEquals("just a parameter", secondDecodedValue.param);
  }

  /** Test whenever the {@link KryoCoder} is serializable. */
  @Test
  public void testCoderSerialization() throws IOException, ClassNotFoundException {
    final KryoRegistrar registrar = k -> k.register(ClassToBeEncoded.class);

    final KryoCoder<ClassToBeEncoded> coder = KryoCoder.of(OPTIONS, registrar);
    final ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    final ObjectOutputStream oss = new ObjectOutputStream(outStr);

    oss.writeObject(coder);
    oss.flush();
    oss.close();

    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(outStr.toByteArray()));
    @SuppressWarnings("unchecked")
    KryoCoder<ClassToBeEncoded> coderDeserialized = (KryoCoder<ClassToBeEncoded>) ois.readObject();

    assertEncoding(coder, coderDeserialized);
  }

  @Test
  public void testCodingWithKvCoderKeyIsKryoCoder() throws IOException {
    final KryoRegistrar registrar = k -> k.register(TestClass.class);

    final ListCoder<Void> listCoder = ListCoder.of(VoidCoder.of());
    final KvCoder<TestClass, List<Void>> kvCoder =
        KvCoder.of(KryoCoder.of(OPTIONS, registrar), listCoder);

    final List<Void> inputValue = new ArrayList<>();
    inputValue.add(null);
    inputValue.add(null);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    final TestClass inputKey = new TestClass("something");
    kvCoder.encode(KV.of(inputKey, inputValue), byteArrayOutputStream);

    final KV<TestClass, List<Void>> decoded =
        kvCoder.decode(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));

    assertNotNull(decoded);
    assertNotNull(decoded.getKey());
    assertEquals(inputKey, decoded.getKey());

    assertNotNull(decoded.getValue());
    assertEquals(inputValue, decoded.getValue());
  }

  @Test
  public void testCodingWithKvCoderValueIsKryoCoder() throws IOException {
    final KryoRegistrar registrar = k -> k.register(TestClass.class);

    final KvCoder<String, TestClass> kvCoder =
        KvCoder.of(StringUtf8Coder.of(), KryoCoder.of(OPTIONS, registrar));

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    final String inputKey = "key";
    final TestClass inputValue = new TestClass("something");
    kvCoder.encode(KV.of(inputKey, inputValue), byteArrayOutputStream);

    final KV<String, TestClass> decoded =
        kvCoder.decode(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));

    assertNotNull(decoded);
    assertNotNull(decoded.getKey());
    assertEquals(inputKey, decoded.getKey());

    assertNotNull(decoded.getValue());
    assertEquals(inputValue, decoded.getValue());
  }

  @Test
  public void testCodingWithKvCoderClassToBeEncoded() throws IOException {
    final KryoRegistrar registrar =
        k -> {
          k.register(TestClass.class);
          k.register(ClassToBeEncoded.class);
        };

    final ListCoder<Void> listCoder = ListCoder.of(VoidCoder.of());
    final KvCoder<ClassToBeEncoded, List<Void>> kvCoder =
        KvCoder.of(KryoCoder.of(OPTIONS, registrar), listCoder);
    final List<Void> inputValue = new ArrayList<>();
    inputValue.add(null);
    inputValue.add(null);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    final ClassToBeEncoded inputKey = new ClassToBeEncoded("something", 1, 0.2);
    kvCoder.encode(KV.of(inputKey, inputValue), byteArrayOutputStream);

    final KV<ClassToBeEncoded, List<Void>> decoded =
        kvCoder.decode(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));

    assertNotNull(decoded);
    assertNotNull(decoded.getKey());
    assertEquals(inputKey, decoded.getKey());

    assertNotNull(decoded.getValue());
    assertEquals(inputValue, decoded.getValue());
  }

  private void assertEncoding(KryoCoder<ClassToBeEncoded> coder) throws IOException {
    assertEncoding(coder, coder);
  }

  private void assertEncoding(
      KryoCoder<ClassToBeEncoded> coderToEncode, KryoCoder<ClassToBeEncoded> coderToDecode)
      throws IOException {
    final ClassToBeEncoded originalValue = new ClassToBeEncoded("XyZ", 42, Double.NaN);
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    coderToEncode.encode(originalValue, outputStream);
    final byte[] buf = outputStream.toByteArray();
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);
    final ClassToBeEncoded decodedValue = coderToDecode.decode(inputStream);
    assertNotNull(decodedValue);
    assertEquals(originalValue, decodedValue);
  }

  private static class ClassToBeEncoded {

    private String firstField;
    private Integer secondField;
    private Double thirdField;

    ClassToBeEncoded(String firstField, Integer secondField, Double thirdField) {
      this.firstField = firstField;
      this.secondField = secondField;
      this.thirdField = thirdField;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ClassToBeEncoded that = (ClassToBeEncoded) o;
      return Objects.equals(firstField, that.firstField)
          && Objects.equals(secondField, that.secondField)
          && Objects.equals(thirdField, that.thirdField);
    }

    @Override
    public int hashCode() {

      return Objects.hash(firstField, secondField, thirdField);
    }
  }

  static class TestClass {

    String param;

    TestClass(String param) {
      this.param = param;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestClass testClass = (TestClass) o;
      return Objects.equals(param, testClass.param);
    }

    @Override
    public int hashCode() {

      return Objects.hash(param);
    }
  }
}
