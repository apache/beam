/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.euphoria.core.translate;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider.Factory;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAware;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeUtils;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.KryoCoder;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.RegisterCoders;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

/** Tests getting coder from functions and type aware functions. */
public class CoderTest {

  private final TranslationContext kryoAllowingContext =
      new TranslationContext(
          mock(DAG.class),
          mock(Factory.class),
          TestPipeline.create(),
          mock(Settings.class),
          Duration.ZERO,
          true);

  private final TranslationContext noFallbackKryoContext =
      new TranslationContext(
          mock(DAG.class),
          mock(Factory.class),
          TestPipeline.create(),
          mock(Settings.class),
          Duration.ZERO,
          false);

  @Test
  public void testGetCoderFromTypeAware() {
    EverythingAwareClass<String> stringAware =
        new EverythingAwareClass<>(TypeDescriptors.strings(), "");

    Coder<String> stringCoder;

    stringCoder = kryoAllowingContext.getOutputCoder(stringAware);
    assertEquals(StringUtf8Coder.class, stringCoder.getClass());

    stringCoder = kryoAllowingContext.getKeyCoder(stringAware);
    assertEquals(StringUtf8Coder.class, stringCoder.getClass());

    stringCoder = kryoAllowingContext.getValueCoder(stringAware);
    assertEquals(StringUtf8Coder.class, stringCoder.getClass());
  }

  @Test
  public void testGetKvCoder() {
    EverythingAwareClass<KV<Integer, String>> stringAware =
        new EverythingAwareClass<>(
            TypeUtils.keyValues(TypeDescriptors.integers(), TypeDescriptors.strings()), "");

    Coder<KV<Integer, String>> kvCoder;

    kvCoder = kryoAllowingContext.getOutputCoder(stringAware);
    assertKVCoder(kvCoder);

    kvCoder = kryoAllowingContext.getKeyCoder(stringAware);
    assertKVCoder(kvCoder);

    kvCoder = kryoAllowingContext.getValueCoder(stringAware);
    assertKVCoder(kvCoder);
  }

  private void assertKVCoder(Coder<KV<Integer, String>> kvCoder) {
    assertEquals(KvCoder.class, kvCoder.getClass());
    KvCoder<Integer, String> castedCoder = (KvCoder<Integer, String>) kvCoder;
    assertEquals(VarIntCoder.class, castedCoder.getKeyCoder().getClass());
    assertEquals(StringUtf8Coder.class, castedCoder.getValueCoder().getClass());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetKvCoderFailsWhenKeyCoderNotAvailable() {
    EverythingAwareClass<KV<ClassWithoutCoder, String>> stringAware =
        new EverythingAwareClass<>(
            TypeUtils.keyValues(
                TypeDescriptor.of(ClassWithoutCoder.class), TypeDescriptors.strings()),
            "");

    noFallbackKryoContext.getOutputCoder(stringAware);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetKvCoderFailsWhenVAlueCoderNotAvailable() {
    EverythingAwareClass<KV<Integer, ClassWithoutCoder>> stringAware =
        new EverythingAwareClass<>(
            TypeUtils.keyValues(
                TypeDescriptors.integers(), TypeDescriptor.of(ClassWithoutCoder.class)),
            "");

    noFallbackKryoContext.getOutputCoder(stringAware);
  }

  @Test(expected = IllegalStateException.class)
  public void testKryoNotAllowed() {
    noFallbackKryoContext.getCoderForTypeOrFallbackCoder(null);
  }

  @Test()
  public void testKryoAllowed() {
    Coder<Object> coder = kryoAllowingContext.getCoderForTypeOrFallbackCoder(null);

    Assert.assertNotNull(coder);
    Assert.assertEquals(KryoCoder.class, coder.getClass());
  }

  @Test(expected = IllegalStateException.class)
  public void testUnregisteredCoderWhenKryoNotAllowed() {
    TypeDescriptor<KV<ClassWithoutCoder, String>> type =
        TypeUtils.keyValues(ClassWithoutCoder.class, String.class);

    noFallbackKryoContext.getCoderForTypeOrFallbackCoder(type);
  }

  @Test()
  public void testUnregisteredCoderWhenKryoAllowed() {
    TypeDescriptor<KV<ClassWithoutCoder, String>> type =
        TypeUtils.keyValues(ClassWithoutCoder.class, String.class);

    Coder<KV<ClassWithoutCoder, String>> coder =
        kryoAllowingContext.getCoderForTypeOrFallbackCoder(type);

    Assert.assertEquals(KryoCoder.class, coder.getClass());
  }

  @Test
  public void testRegisteredCoders() {
    Pipeline p = TestPipeline.create();

    BeamFlow flow = BeamFlow.of("testFlow", p, true);

    RegisterCoders.to(flow).registerCoder(TestCodeableClass.class, new TestCoder()).done();

    TranslationContext context = flow.getTranslationContext();

    Coder<TestCodeableClass> coder =
        context.getCoderForTypeOrFallbackCoder(TypeDescriptor.of(TestCodeableClass.class));

    Assert.assertEquals(TestCoder.class, coder.getClass());
  }

  private static class ClassWithoutCoder {}

  private static class EverythingAwareClass<T>
      implements TypeAware.Key<T>, TypeAware.Value<T>, TypeAware.Output<T> {
    private final TypeDescriptor<T> typeToReturn;
    private final String name;

    public EverythingAwareClass(TypeDescriptor<T> typeToReturn, String name) {
      this.typeToReturn = typeToReturn;
      this.name = name;
    }

    @Override
    public TypeDescriptor<T> getKeyType() {
      return typeToReturn;
    }

    @Override
    public TypeDescriptor<T> getOutputType() {
      return typeToReturn;
    }

    @Override
    public TypeDescriptor<T> getValueType() {
      return typeToReturn;
    }

    @Override
    public String getName() {
      return name;
    }
  }

  private static class TestCodeableClass {};

  private static class TestCoder extends Coder<TestCodeableClass> {

    @Override
    public void encode(TestCodeableClass value, OutputStream outStream)
        throws CoderException, IOException {}

    @Override
    public TestCodeableClass decode(InputStream inStream) throws CoderException, IOException {
      return null;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }
}
