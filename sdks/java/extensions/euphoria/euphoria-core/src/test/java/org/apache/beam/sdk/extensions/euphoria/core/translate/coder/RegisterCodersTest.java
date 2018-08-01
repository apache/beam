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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
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
        .registerCoder(FirstTestDataType.class, firstCoder)
        .registerCoder(SecondTestDataType.class)
        .registerCoder(new TypeDescriptor<ParametrizedTestDataType<String>>() {}, parametrizedCoder)
        .done();

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();

    Assert.assertSame(firstCoder, coderRegistry.getCoder(FirstTestDataType.class));

    Coder<SecondTestDataType> actualSecondTypeCoder =
        coderRegistry.getCoder(SecondTestDataType.class);
    Assert.assertTrue(actualSecondTypeCoder instanceof ClassAwareKryoCoder);

    Coder<ParametrizedTestDataType<String>> parametrizedTypeActualCoder =
        coderRegistry.getCoder(new TypeDescriptor<ParametrizedTestDataType<String>>() {});
    Assert.assertSame(parametrizedCoder, parametrizedTypeActualCoder);
  }

  private static class FirstTestDataType {}

  private static class SecondTestDataType {}

  private static class ParametrizedTestDataType<T> {}

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
