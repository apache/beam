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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/** Unit test of {@link RegisterCoders}. */
public class RegisterCodersTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testCodersRegistration() throws CannotProvideCoderException {

    DummyTestCoder<FirstTestDataType> firstCoder = new DummyTestCoder<>();
    DummyTestCoder<ParametrizedTestDataType<String>> parametrizedCoder = new DummyTestCoder<>();

    RegisterCoders.to(pipeline)
        .setKryoClassRegistrar(
            (k) -> {
              k.register(KryoSerializedTestType.class);
            })
        .registerCoder(FirstTestDataType.class, firstCoder)
        .registerCoder(new TypeDescriptor<ParametrizedTestDataType<String>>() {}, parametrizedCoder)
        .done();

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();

    Assert.assertSame(firstCoder, coderRegistry.getCoder(FirstTestDataType.class));

    Coder<KryoSerializedTestType> actualKryoTypeCoder =
        coderRegistry.getCoder(KryoSerializedTestType.class);
    Assert.assertTrue(actualKryoTypeCoder instanceof KryoCoder);

    Coder<ParametrizedTestDataType<String>> parametrizedTypeActualCoder =
        coderRegistry.getCoder(new TypeDescriptor<ParametrizedTestDataType<String>>() {});
    Assert.assertSame(parametrizedCoder, parametrizedTypeActualCoder);
  }

  @Test
  public void testKryoCoderTheSameSecondTime() throws CannotProvideCoderException {

    RegisterCoders.to(pipeline)
        .setKryoClassRegistrar(
            (k) -> {
              k.register(KryoSerializedTestType.class);
            })
        .done();

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();

    Coder<KryoSerializedTestType> firstReturnedKryoTypeCoder =
        coderRegistry.getCoder(KryoSerializedTestType.class);
    Assert.assertTrue(firstReturnedKryoTypeCoder instanceof KryoCoder);

    Coder<KryoSerializedTestType> secondReturnedKryoTypeCoder =
        coderRegistry.getCoder(KryoSerializedTestType.class);
    Assert.assertTrue(secondReturnedKryoTypeCoder instanceof KryoCoder);

    Assert.assertSame(firstReturnedKryoTypeCoder, secondReturnedKryoTypeCoder);
  }

  @Test
  public void testKryoRegisterCodeDecode() throws CannotProvideCoderException, IOException {

    RegisterCoders.to(pipeline)
        .setKryoClassRegistrar(
            (k) -> {
              k.register(SimpleDataClassWithEquality.class);
            })
        .done();

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();

    Coder<SimpleDataClassWithEquality> coder =
        coderRegistry.getCoder(SimpleDataClassWithEquality.class);
    Assert.assertTrue(coder instanceof KryoCoder);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    SimpleDataClassWithEquality originalValue = new SimpleDataClassWithEquality("asdkjsadui");

    coder.encode(originalValue, outputStream);

    byte[] buf = outputStream.toByteArray();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);

    SimpleDataClassWithEquality decodedValue = coder.decode(inputStream);

    Assert.assertNotNull(decodedValue);
    Assert.assertEquals(originalValue, decodedValue);
  }

  private static class FirstTestDataType {}

  private static class KryoSerializedTestType {}

  private static class ParametrizedTestDataType<T> {}

  private static class SimpleDataClassWithEquality {

    private final String someField;

    public SimpleDataClassWithEquality(String someField) {
      this.someField = someField;
    }

    public String getSomeField() {
      return someField;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SimpleDataClassWithEquality that = (SimpleDataClassWithEquality) o;
      return Objects.equals(someField, that.someField);
    }

    @Override
    public int hashCode() {

      return Objects.hash(someField);
    }
  }

  private static class DummyTestCoder<T> extends Coder<T> {

    @Override
    public void encode(T value, OutputStream outStream) throws CoderException, IOException {
      throwCoderException();
    }

    @Override
    public T decode(InputStream inStream) throws CoderException, IOException {
      throwCoderException();
      return null;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      throwRuntimeException();
      return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      throwRuntimeException();
    }

    private void throwCoderException() throws CoderException {
      throw new CoderException(
          DummyTestCoder.class.getSimpleName() + " is supposed to do nothing.");
    }

    private void throwRuntimeException() throws RuntimeException {
      throw new RuntimeException(
          DummyTestCoder.class.getSimpleName() + " is supposed to do nothing.");
    }
  }
}
