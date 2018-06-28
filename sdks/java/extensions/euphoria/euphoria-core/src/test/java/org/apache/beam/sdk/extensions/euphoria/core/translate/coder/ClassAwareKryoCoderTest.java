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
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test targeted at {@link ClassAwareKryoCoder}.
 */
public class ClassAwareKryoCoderTest {

  @Test
  public void testCoding() throws IOException {
    ClassAwareKryoCoder<ClassToBeEncoded> coder = new ClassAwareKryoCoder<>(ClassToBeEncoded.class);
    assertEncoding(coder);
  }

  @Test
  public void testCoderSerialization() throws IOException, ClassNotFoundException {
    ClassAwareKryoCoder<ClassToBeEncoded> coder = new ClassAwareKryoCoder<>(ClassToBeEncoded.class);
    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    ObjectOutputStream oss = new ObjectOutputStream(outStr);

    oss.writeObject(coder);
    oss.flush();
    oss.close();

    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(outStr.toByteArray()));
    ClassAwareKryoCoder<ClassToBeEncoded> coderDeserialized =
        (ClassAwareKryoCoder<ClassToBeEncoded>) ois.readObject();

    assertEncoding(coderDeserialized);
  }

  @Test
  public void testCodingWithKVcoder() throws IOException {

    final ListCoder<Void> listCoder = ListCoder.of(VoidCoder.of());
    final KvCoder<TestClass, List<Void>> kvCoder = KvCoder
        .of(ClassAwareKryoCoder.of(TestClass.class), listCoder);
    List<Void> a = new ArrayList<>();
    a.add(null);
    a.add(null);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(2048);

    kvCoder.encode(KV.of(new TestClass("something"), a), byteArrayOutputStream);

    final KV<TestClass, List<Void>> decode = kvCoder
        .decode(new ByteArrayInputStream(byteArrayOutputStream
            .toByteArray()));
    System.out.println("decode = " + decode);
  }

  private void assertEncoding(ClassAwareKryoCoder<ClassToBeEncoded> coder)
      throws IOException {
    ClassToBeEncoded originalValue = new ClassToBeEncoded("XyZ", 42, Double.NaN);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    coder.encode(originalValue, outputStream);

    byte[] buf = outputStream.toByteArray();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);

    ClassToBeEncoded decodedValue = coder.decode(inputStream);

    Assert.assertNotNull(decodedValue);
    Assert.assertEquals(originalValue, decodedValue);
  }


  private static class ClassToBeEncoded {

    private String firstField;
    private Integer secondField;
    private Double thirdField;

    public ClassToBeEncoded(String firstField, Integer secondField, Double thirdField) {
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

    public TestClass(String param) {
      this.param = param;
    }

  }
}
