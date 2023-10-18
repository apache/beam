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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.CoderRegistry.IncompatibleCoderException;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for CoderRegistry. */
@RunWith(JUnit4.class)
public class CoderRegistryTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(CoderRegistry.class);

  @Test
  public void testRegisterInstantiatedCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    registry.registerCoderForClass(MyValue.class, MyValueCoder.of());
    assertEquals(registry.getCoder(MyValue.class), MyValueCoder.of());
  }

  @Test
  public void testSimpleDefaultCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    assertEquals(StringUtf8Coder.of(), registry.getCoder(String.class));
    assertEquals(VarIntCoder.of(), registry.getCoder(Integer.class));
    assertEquals(VarLongCoder.of(), registry.getCoder(Long.class));
    assertEquals(FloatCoder.of(), registry.getCoder(Float.class));
    assertEquals(DoubleCoder.of(), registry.getCoder(Double.class));
  }

  @Test
  public void testSimpleUnknownDefaultCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    thrown.expect(CannotProvideCoderException.class);
    thrown.expectMessage(
        allOf(
            containsString(UnknownType.class.getName()),
            containsString("Unable to provide a Coder for")));
    registry.getCoder(UnknownType.class);
  }

  @Test
  public void testParameterizedDefaultListCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<List<Integer>> listToken = new TypeDescriptor<List<Integer>>() {};
    assertEquals(ListCoder.of(VarIntCoder.of()), registry.getCoder(listToken));

    registry.registerCoderProvider(
        CoderProviders.fromStaticMethods(MyValue.class, MyValueCoder.class));
    TypeDescriptor<KV<String, List<MyValue>>> kvToken =
        new TypeDescriptor<KV<String, List<MyValue>>>() {};
    assertEquals(
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(MyValueCoder.of())),
        registry.getCoder(kvToken));
  }

  @Test
  public void testParameterizedDefaultMapCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<Map<Integer, String>> mapToken = new TypeDescriptor<Map<Integer, String>>() {};
    assertEquals(MapCoder.of(VarIntCoder.of(), StringUtf8Coder.of()), registry.getCoder(mapToken));
  }

  @Test
  public void testParameterizedDefaultNestedMapCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<Map<Integer, Map<String, Double>>> mapToken =
        new TypeDescriptor<Map<Integer, Map<String, Double>>>() {};
    assertEquals(
        MapCoder.of(VarIntCoder.of(), MapCoder.of(StringUtf8Coder.of(), DoubleCoder.of())),
        registry.getCoder(mapToken));
  }

  @Test
  public void testParameterizedDefaultSetCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<Set<Integer>> setToken = new TypeDescriptor<Set<Integer>>() {};
    assertEquals(SetCoder.of(VarIntCoder.of()), registry.getCoder(setToken));
  }

  @Test
  public void testParameterizedDefaultNestedSetCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<Set<Set<Integer>>> setToken = new TypeDescriptor<Set<Set<Integer>>>() {};
    assertEquals(SetCoder.of(SetCoder.of(VarIntCoder.of())), registry.getCoder(setToken));
  }

  @Test
  public void testParameterizedDefaultCoderUnknown() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<List<UnknownType>> listUnknownToken = new TypeDescriptor<List<UnknownType>>() {};

    thrown.expect(CannotProvideCoderException.class);
    thrown.expectMessage(
        String.format(
            "Cannot provide coder for parameterized type %s: Unable to provide a Coder for %s",
            listUnknownToken, UnknownType.class.getName()));

    registry.getCoder(listUnknownToken);
  }

  @Test
  public void testParameterizedWildcardTypeIsUnknown() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<List<? extends MyValue>> wildcardUnknownToken =
        new TypeDescriptor<List<? extends MyValue>>() {};

    thrown.expect(CannotProvideCoderException.class);
    thrown.expectMessage(
        String.format(
            "Cannot provide coder for parameterized type %s: Cannot provide a coder for wildcard type %s.",
            wildcardUnknownToken,
            ((ParameterizedType) wildcardUnknownToken.getType()).getActualTypeArguments()[0]));
    registry.getCoder(wildcardUnknownToken);
  }

  @Test
  public void testTypeParameterInferenceForward() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    MyGenericClass<MyValue, List<MyValue>> instance =
        new MyGenericClass<MyValue, List<MyValue>>() {};

    Coder<?> bazCoder =
        registry.getCoder(
            instance.getClass(),
            MyGenericClass.class,
            Collections.<Type, Coder<?>>singletonMap(
                TypeDescriptor.of(MyGenericClass.class).getTypeParameter("FooT"),
                MyValueCoder.of()),
            TypeDescriptor.of(MyGenericClass.class).getTypeParameter("BazT"));

    assertEquals(ListCoder.of(MyValueCoder.of()), bazCoder);
  }

  @Test
  public void testTypeParameterInferenceBackward() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    MyGenericClass<MyValue, List<MyValue>> instance =
        new MyGenericClass<MyValue, List<MyValue>>() {};

    Coder<?> fooCoder =
        registry.getCoder(
            instance.getClass(),
            MyGenericClass.class,
            Collections.<Type, Coder<?>>singletonMap(
                TypeDescriptor.of(MyGenericClass.class).getTypeParameter("BazT"),
                ListCoder.of(MyValueCoder.of())),
            TypeDescriptor.of(MyGenericClass.class).getTypeParameter("FooT"));

    assertEquals(MyValueCoder.of(), fooCoder);
  }

  @Test
  public void testTypeCompatibility() throws Exception {
    CoderRegistry.verifyCompatible(BigEndianIntegerCoder.of(), Integer.class);
    CoderRegistry.verifyCompatible(
        ListCoder.of(BigEndianIntegerCoder.of()), new TypeDescriptor<List<Integer>>() {}.getType());
  }

  @Test
  public void testIntVersusStringIncompatibility() throws Exception {
    thrown.expect(IncompatibleCoderException.class);
    thrown.expectMessage("not assignable");
    CoderRegistry.verifyCompatible(BigEndianIntegerCoder.of(), String.class);
  }

  // BEAM-3160: We can't choose between the BigEndianIntegerCoder or the VarIntCoder since
  // they are incompatible.
  @Test
  public void testTypeOverSpecifiedWithMultipleCoders() throws Exception {
    thrown.expect(CannotProvideCoderException.class);
    thrown.expectMessage("type is over specified");
    CoderRegistry.createDefault()
        .getCoder(
            new TypeDescriptor<Integer>() {},
            new TypeDescriptor<KV<Integer, Integer>>() {},
            KvCoder.of(BigEndianIntegerCoder.of(), VarIntCoder.of()));
  }

  private static class TooManyComponentCoders<T> extends ListCoder<T> {
    public TooManyComponentCoders(Coder<T> actualComponentCoder) {
      super(actualComponentCoder);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return ImmutableList.<Coder<?>>builder()
          .addAll(super.getCoderArguments())
          .add(BigEndianLongCoder.of())
          .build();
    }
  }

  @Test
  public void testTooManyCoderArguments() throws Exception {
    thrown.expect(IncompatibleCoderException.class);
    thrown.expectMessage("type parameters");
    thrown.expectMessage("less than the number of coder arguments");
    CoderRegistry.verifyCompatible(
        new TooManyComponentCoders<>(BigEndianIntegerCoder.of()), List.class);
  }

  @Test
  public void testComponentIncompatibility() throws Exception {
    thrown.expect(IncompatibleCoderException.class);
    thrown.expectMessage("component coder is incompatible");
    CoderRegistry.verifyCompatible(
        ListCoder.of(BigEndianIntegerCoder.of()), new TypeDescriptor<List<String>>() {}.getType());
  }

  @Test
  public void testDefaultCoderAnnotationGenericRawtype() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    assertEquals(
        registry.getCoder(MySerializableGeneric.class),
        SerializableCoder.of(MySerializableGeneric.class));
  }

  @Test
  public void testDefaultCoderAnnotationGeneric() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    assertEquals(
        registry.getCoder(new TypeDescriptor<MySerializableGeneric<String>>() {}),
        SerializableCoder.of(MySerializableGeneric.class));
  }

  private static class PTransformOutputingMySerializableGeneric
      extends PTransform<
          PCollection<String>, PCollection<KV<String, MySerializableGeneric<String>>>> {

    private static class OutputDoFn
        extends DoFn<String, KV<String, MySerializableGeneric<String>>> {
      @ProcessElement
      public void processElement(ProcessContext c) {}
    }

    @Override
    public PCollection<KV<String, MySerializableGeneric<String>>> expand(
        PCollection<String> input) {
      return input.apply(ParDo.of(new PTransformOutputingMySerializableGeneric.OutputDoFn()));
    }
  }

  /**
   * Tests that the error message for a type variable includes a mention of where the type variable
   * was declared.
   */
  @Test
  public void testTypeVariableErrorMessage() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();

    thrown.expect(CannotProvideCoderException.class);
    thrown.expectMessage("Unable to provide a Coder");
    registry.getCoder(TypeDescriptor.of(TestGenericClass.class.getTypeParameters()[0]));
  }

  private static class TestGenericClass<TestGenericT> {}

  @Test
  @SuppressWarnings("rawtypes")
  public void testSerializableTypeVariableDefaultCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();

    TypeDescriptor type =
        TypeDescriptor.of(TestSerializableGenericClass.class.getTypeParameters()[0]);
    assertEquals(SerializableCoder.of(type), registry.getCoder(type));
  }

  private static class TestSerializableGenericClass<TestGenericT extends Serializable> {}

  /**
   * In-context test that assures the functionality tested in {@link
   * #testDefaultCoderAnnotationGeneric} is invoked in the right ways.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testSpecializedButIgnoredGenericInPipeline() throws Exception {

    pipeline
        .apply(Create.of("hello", "goodbye"))
        .apply(new PTransformOutputingMySerializableGeneric());

    pipeline.run();
  }

  private static class GenericOutputMySerializedGeneric<T extends Serializable>
      extends PTransform<PCollection<String>, PCollection<KV<String, MySerializableGeneric<T>>>> {

    private class OutputDoFn extends DoFn<String, KV<String, MySerializableGeneric<T>>> {
      @ProcessElement
      public void processElement(ProcessContext c) {}
    }

    @Override
    public PCollection<KV<String, MySerializableGeneric<T>>> expand(PCollection<String> input) {
      return input.apply(ParDo.of(new OutputDoFn()));
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIgnoredGenericInPipeline() throws Exception {

    pipeline
        .apply(Create.of("hello", "goodbye"))
        .apply(new GenericOutputMySerializedGeneric<String>());

    pipeline.run();
  }

  private static class MyGenericClass<FooT, BazT> {}

  private static class MyValue {}

  private static class MyValueCoder extends AtomicCoder<MyValue> {

    private static final MyValueCoder INSTANCE = new MyValueCoder();
    private static final TypeDescriptor<MyValue> TYPE_DESCRIPTOR = TypeDescriptor.of(MyValue.class);

    public static MyValueCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(MyValue value, OutputStream outStream) throws CoderException, IOException {}

    @Override
    public MyValue decode(InputStream inStream) throws CoderException, IOException {
      return new MyValue();
    }

    @Override
    public void verifyDeterministic() {}

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public Object structuralValue(MyValue value) {
      return value;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(MyValue value) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(MyValue value, ElementByteSizeObserver observer)
        throws Exception {
      observer.update(0L);
    }

    @Override
    public TypeDescriptor<MyValue> getEncodedTypeDescriptor() {
      return TYPE_DESCRIPTOR;
    }
  }

  /**
   * This type is incompatible with all known coder providers such as Serializable,
   * {@code @DefaultCoder} which allows testing scenarios where coder lookup fails.
   */
  private static class UnknownType {}

  @DefaultCoder(SerializableCoder.class)
  private static class MySerializableGeneric<T extends Serializable> implements Serializable {
    @SuppressWarnings("unused")
    private T foo;
  }

  /**
   * This type is incompatible with all known coder providers such as Serializable,
   * {@code @DefaultCoder} which allows testing the automatic registration mechanism.
   */
  private static class AutoRegistrationClass {}

  private static class AutoRegistrationClassCoder extends CustomCoder<AutoRegistrationClass> {
    private static final AutoRegistrationClassCoder INSTANCE = new AutoRegistrationClassCoder();

    @Override
    public void encode(AutoRegistrationClass value, OutputStream outStream) {}

    @Override
    public AutoRegistrationClass decode(InputStream inStream) {
      return null;
    }
  }

  @Test
  public void testAutomaticRegistrationOfCoderProviders() throws Exception {
    assertEquals(
        AutoRegistrationClassCoder.INSTANCE,
        CoderRegistry.createDefault().getCoder(AutoRegistrationClass.class));
  }

  /** A {@link CoderProviderRegistrar} to demonstrate default {@link Coder} registration. */
  @AutoService(CoderProviderRegistrar.class)
  public static class RegisteredTestCoderProviderRegistrar implements CoderProviderRegistrar {
    @Override
    public List<CoderProvider> getCoderProviders() {
      return ImmutableList.of(
          CoderProviders.forCoder(
              TypeDescriptor.of(AutoRegistrationClass.class), AutoRegistrationClassCoder.INSTANCE));
    }
  }

  @Test
  public void testCoderPrecedence() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();

    // DefaultCoder precedes CoderProviderRegistrar
    assertEquals(MockDefaultCoder.of(MyValueA.class), registry.getCoder(MyValueA.class));

    // CoderProviderRegistrar precedes SerializableCoder
    assertEquals(MyValueBCoder.INSTANCE, registry.getCoder(MyValueB.class));

    // fallbacks to SerializableCoder at last
    assertEquals(SerializableCoder.of(MyValueC.class), registry.getCoder(MyValueC.class));
  }

  @DefaultCoder(MockDefaultCoder.class)
  private static class MyValueA implements Serializable {}

  private static class MyValueB implements Serializable {}

  private static class MyValueC implements Serializable {}

  private static class MyValueACoder extends CustomCoder<MyValueA> {
    private static final MyValueACoder INSTANCE = new MyValueACoder();

    @Override
    public void encode(MyValueA value, OutputStream outStream) throws CoderException, IOException {}

    @Override
    public MyValueA decode(InputStream inStream) throws CoderException, IOException {
      return null;
    }
  }

  /** A {@link CoderProviderRegistrar} to demonstrate default {@link Coder} registration. */
  @AutoService(CoderProviderRegistrar.class)
  public static class MyValueACoderProviderRegistrar implements CoderProviderRegistrar {
    @Override
    public List<CoderProvider> getCoderProviders() {
      return ImmutableList.of(
          CoderProviders.forCoder(TypeDescriptor.of(MyValueA.class), MyValueACoder.INSTANCE));
    }
  }

  private static class MyValueBCoder extends CustomCoder<MyValueB> {
    private static final MyValueBCoder INSTANCE = new MyValueBCoder();

    @Override
    public void encode(MyValueB value, OutputStream outStream) throws CoderException, IOException {}

    @Override
    public MyValueB decode(InputStream inStream) throws CoderException, IOException {
      return null;
    }
  }

  /** A {@link CoderProviderRegistrar} to demonstrate default {@link Coder} registration. */
  @AutoService(CoderProviderRegistrar.class)
  public static class MyValueBCoderProviderRegistrar implements CoderProviderRegistrar {
    @Override
    public List<CoderProvider> getCoderProviders() {
      return ImmutableList.of(
          CoderProviders.forCoder(TypeDescriptor.of(MyValueB.class), MyValueBCoder.INSTANCE));
    }
  }
}
