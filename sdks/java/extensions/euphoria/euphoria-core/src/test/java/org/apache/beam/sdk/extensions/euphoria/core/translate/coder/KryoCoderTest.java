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
package org.apache.beam.sdk.extensions.euphoria.core.translate.coder;

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
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test targeted at {@link KryoCoder}.
 */
public class KryoCoderTest {

  @Test
  public void testBasicCoding() throws IOException {

    KryoRegistrar registrar = (k) -> k.register(ClassToBeEncoded.class);

    KryoCoder<ClassToBeEncoded> coder = KryoCoder.of(registrar);
    assertEncoding(coder);
  }

  @Test(expected = CoderException.class)
  public void testWrongRegistrarCoding() throws IOException {

    KryoRegistrar registrar = (k) -> { /* No-op  */};

    KryoCoder<ClassToBeEncoded> coder = KryoCoder.of(registrar);
    assertEncoding(coder);
  }

  @Test(expected = CoderException.class)
  public void testWrongRegistrarDecoding() throws IOException {

    KryoRegistrar registrarCoding = (k) -> k.register(ClassToBeEncoded.class);
    KryoRegistrar registrarDecoding = (k) -> { /* No-op  */};

    KryoCoder<ClassToBeEncoded> coderToEncode = KryoCoder.of(registrarCoding);
    KryoCoder<ClassToBeEncoded> coderToDecode = KryoCoder.of(registrarDecoding);

    assertEncoding(coderToEncode, coderToDecode);
  }

  @Test
  public void testCodingOfTwoClassesInSerial() throws IOException {
    KryoRegistrar registrar = (k) -> {
      k.register(ClassToBeEncoded.class);
      k.register(TestClass.class);
    };

    KryoCoder<ClassToBeEncoded> coder = KryoCoder.of(registrar);
    KryoCoder<TestClass> secondCoder = KryoCoder.of(registrar);

    ClassToBeEncoded originalValue = new ClassToBeEncoded("XyZ", 42, Double.NaN);
    TestClass secondOriginalValue = new TestClass("just a parameter");
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    coder.encode(originalValue, outputStream);
    secondCoder.encode(secondOriginalValue, outputStream);

    byte[] buf = outputStream.toByteArray();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);

    ClassToBeEncoded decodedValue = coder.decode(inputStream);
    TestClass secondDecodedValue = secondCoder.decode(inputStream);

    Assert.assertNotNull(decodedValue);
    Assert.assertEquals(originalValue, decodedValue);

    Assert.assertNotNull(secondDecodedValue);
    Assert.assertNotNull(secondDecodedValue.param);
    Assert.assertEquals("just a parameter", secondDecodedValue.param);
  }

  /**
   * Test whenever the {@link KryoCoder} is serializable.
   */
  @Test
  public void testCoderSerialization() throws IOException, ClassNotFoundException {
    KryoRegistrar registrar = (k) -> k.register(ClassToBeEncoded.class);

    KryoCoder<ClassToBeEncoded> coder = KryoCoder.of(registrar);
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    ObjectOutputStream oss = new ObjectOutputStream(outStr);

    oss.writeObject(coder);
    oss.flush();
    oss.close();

    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(outStr.toByteArray()));
    KryoCoder<ClassToBeEncoded> coderDeserialized = (KryoCoder<ClassToBeEncoded>) ois.readObject();

    assertEncoding(coder, coderDeserialized);
  }


  @Test
  public void testCodingWithKvCoderKeyIsKryoCoder() throws IOException {
    KryoRegistrar registrar = (k) -> k.register(TestClass.class);

    final ListCoder<Void> listCoder = ListCoder.of(VoidCoder.of());
    final KvCoder<TestClass, List<Void>> kvCoder = KvCoder
        .of(KryoCoder.of(registrar), listCoder);

    List<Void> inputValue = new ArrayList<>();
    inputValue.add(null);
    inputValue.add(null);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    TestClass inputKey = new TestClass("something");
    kvCoder.encode(KV.of(inputKey, inputValue), byteArrayOutputStream);

    final KV<TestClass, List<Void>> decoded = kvCoder
        .decode(new ByteArrayInputStream(byteArrayOutputStream
            .toByteArray()));

    Assert.assertNotNull(decoded);
    Assert.assertNotNull(decoded.getKey());
    Assert.assertEquals(inputKey, decoded.getKey());

    Assert.assertNotNull(decoded.getValue());
    Assert.assertEquals(inputValue, decoded.getValue());

  }

  @Test
  public void testCodingWithKvCoderValueIsKryoCoder() throws IOException {
    KryoRegistrar registrar = (k) -> k.register(TestClass.class);

    final KvCoder<String, TestClass> kvCoder = KvCoder
        .of(StringUtf8Coder.of(), KryoCoder.of(registrar));

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    String inputKey = "key";
    TestClass inputValue = new TestClass("something");
    kvCoder.encode(KV.of(inputKey, inputValue), byteArrayOutputStream);

    final KV<String, TestClass> decoded = kvCoder
        .decode(new ByteArrayInputStream(byteArrayOutputStream
            .toByteArray()));

    Assert.assertNotNull(decoded);
    Assert.assertNotNull(decoded.getKey());
    Assert.assertEquals(inputKey, decoded.getKey());

    Assert.assertNotNull(decoded.getValue());
    Assert.assertEquals(inputValue, decoded.getValue());
  }

  @Test
  public void testCodingWithKvCoderClassToBeEncoded() throws IOException {
    KryoRegistrar registrar = (k) -> {
      k.register(TestClass.class);
      k.register(ClassToBeEncoded.class);
    };

    final ListCoder<Void> listCoder = ListCoder.of(VoidCoder.of());
    final KvCoder<ClassToBeEncoded, List<Void>> kvCoder =
        KvCoder.of(KryoCoder.of(registrar), listCoder);
    List<Void> inputValue = new ArrayList<>();
    inputValue.add(null);
    inputValue.add(null);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    ClassToBeEncoded inputKey = new ClassToBeEncoded("something", 1, 0.2);
    kvCoder.encode(KV.of(inputKey, inputValue), byteArrayOutputStream);

    final KV<ClassToBeEncoded, List<Void>> decoded = kvCoder
        .decode(new ByteArrayInputStream(byteArrayOutputStream
            .toByteArray()));

    Assert.assertNotNull(decoded);
    Assert.assertNotNull(decoded.getKey());
    Assert.assertEquals(inputKey, decoded.getKey());

    Assert.assertNotNull(decoded.getValue());
    Assert.assertEquals(inputValue, decoded.getValue());
  }

  private void assertEncoding(KryoCoder<ClassToBeEncoded> coder) throws IOException {
    assertEncoding(coder, coder);
  }

  private void assertEncoding(KryoCoder<ClassToBeEncoded> coderToEncode,
      KryoCoder<ClassToBeEncoded> coderToDecode) throws IOException {

    ClassToBeEncoded originalValue = new ClassToBeEncoded("XyZ", 42, Double.NaN);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    coderToEncode.encode(originalValue, outputStream);

    byte[] buf = outputStream.toByteArray();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);

    ClassToBeEncoded decodedValue = coderToDecode.decode(inputStream);

    Assert.assertNotNull(decodedValue);
    Assert.assertEquals(originalValue, decodedValue);
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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
