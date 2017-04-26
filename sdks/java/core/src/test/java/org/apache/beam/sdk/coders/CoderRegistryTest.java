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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Arrays;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for CoderRegistry.
 */
@RunWith(JUnit4.class)
public class CoderRegistryTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public ExpectedLogs expectedLogs = ExpectedLogs.none(CoderRegistry.class);

  private static class SerializableClass implements Serializable {
  }

  private static class NotSerializableClass { }

  @Test
  public void testSerializableFallbackCoderProvider() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    registry.setFallbackCoderProvider(SerializableCoder.PROVIDER);
    Coder<?> serializableCoder = registry.getDefaultCoder(SerializableClass.class);

    assertEquals(serializableCoder, SerializableCoder.of(SerializableClass.class));
  }

  @Test
  public void testAvroFallbackCoderProvider() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    registry.setFallbackCoderProvider(AvroCoder.PROVIDER);
    Coder<?> avroCoder = registry.getDefaultCoder(NotSerializableClass.class);

    assertEquals(avroCoder, AvroCoder.of(NotSerializableClass.class));
  }

  @Test
  public void testRegisterInstantiatedCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    registry.registerCoder(MyValue.class, MyValueCoder.of());
    assertEquals(registry.getDefaultCoder(MyValue.class), MyValueCoder.of());
  }

  @SuppressWarnings("rawtypes") // this class exists to fail a test because of its rawtypes
  private class MyListCoder extends CustomCoder<List> {
    @Override
    public void encode(List value, OutputStream outStream, Context context)
        throws CoderException, IOException {
    }

    @Override
    public List decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      return Collections.emptyList();
    }

    @Override
    public List<Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  @Test
  public void testRegisterInstantiatedCoderInvalidRawtype() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("may not be used with unspecialized generic classes");
    CoderRegistry registry = CoderRegistry.createDefault();
    registry.registerCoder(List.class, new MyListCoder());
  }

  @Test
  public void testSimpleDefaultCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    assertEquals(StringUtf8Coder.of(), registry.getDefaultCoder(String.class));
  }

  @Test
  public void testSimpleUnknownDefaultCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    thrown.expect(CannotProvideCoderException.class);
    thrown.expectMessage(allOf(
        containsString(UnknownType.class.getCanonicalName()),
        containsString("No CoderFactory has been registered"),
        containsString("does not have a @DefaultCoder annotation"),
        containsString("does not implement Serializable")));
    registry.getDefaultCoder(UnknownType.class);
  }

  @Test
  public void testParameterizedDefaultListCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<List<Integer>> listToken = new TypeDescriptor<List<Integer>>() {};
    assertEquals(ListCoder.of(VarIntCoder.of()),
                 registry.getDefaultCoder(listToken));

    registry.registerCoder(MyValue.class, MyValueCoder.class);
    TypeDescriptor<KV<String, List<MyValue>>> kvToken =
        new TypeDescriptor<KV<String, List<MyValue>>>() {};
    assertEquals(KvCoder.of(StringUtf8Coder.of(),
                            ListCoder.of(MyValueCoder.of())),
                 registry.getDefaultCoder(kvToken));

  }

  @Test
  public void testParameterizedDefaultMapCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<Map<Integer, String>> mapToken = new TypeDescriptor<Map<Integer, String>>() {};
    assertEquals(MapCoder.of(VarIntCoder.of(), StringUtf8Coder.of()),
                 registry.getDefaultCoder(mapToken));
  }

  @Test
  public void testParameterizedDefaultNestedMapCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<Map<Integer, Map<String, Double>>> mapToken =
        new TypeDescriptor<Map<Integer, Map<String, Double>>>() {};
    assertEquals(
        MapCoder.of(VarIntCoder.of(), MapCoder.of(StringUtf8Coder.of(), DoubleCoder.of())),
        registry.getDefaultCoder(mapToken));
  }

  @Test
  public void testParameterizedDefaultSetCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<Set<Integer>> setToken = new TypeDescriptor<Set<Integer>>() {};
    assertEquals(SetCoder.of(VarIntCoder.of()), registry.getDefaultCoder(setToken));
  }

  @Test
  public void testParameterizedDefaultNestedSetCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<Set<Set<Integer>>> setToken = new TypeDescriptor<Set<Set<Integer>>>() {};
    assertEquals(SetCoder.of(SetCoder.of(VarIntCoder.of())), registry.getDefaultCoder(setToken));
  }

  @Test
  public void testParameterizedDefaultCoderUnknown() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    TypeDescriptor<List<UnknownType>> listUnknownToken = new TypeDescriptor<List<UnknownType>>() {};

    thrown.expect(CannotProvideCoderException.class);
    thrown.expectMessage(String.format(
        "Cannot provide coder for parameterized type %s: Unable to provide a default Coder for %s",
        listUnknownToken,
        UnknownType.class.getCanonicalName()));

    registry.getDefaultCoder(listUnknownToken);
  }

  @Test
  public void testTypeParameterInferenceForward() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    MyGenericClass<MyValue, List<MyValue>> instance =
        new MyGenericClass<MyValue, List<MyValue>>() {};

    Coder<?> bazCoder = registry.getDefaultCoder(
        instance.getClass(),
        MyGenericClass.class,
        Collections.<Type, Coder<?>>singletonMap(
            TypeDescriptor.of(MyGenericClass.class).getTypeParameter("FooT"), MyValueCoder.of()),
        TypeDescriptor.of(MyGenericClass.class).getTypeParameter("BazT"));

    assertEquals(ListCoder.of(MyValueCoder.of()), bazCoder);
  }

  @Test
  public void testTypeParameterInferenceBackward() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    MyGenericClass<MyValue, List<MyValue>> instance =
        new MyGenericClass<MyValue, List<MyValue>>() {};

    Coder<?> fooCoder = registry.getDefaultCoder(
        instance.getClass(),
        MyGenericClass.class,
        Collections.<Type, Coder<?>>singletonMap(
            TypeDescriptor.of(MyGenericClass.class).getTypeParameter("BazT"),
            ListCoder.of(MyValueCoder.of())),
        TypeDescriptor.of(MyGenericClass.class).getTypeParameter("FooT"));

    assertEquals(MyValueCoder.of(), fooCoder);
  }

  @Test
  public void testGetDefaultCoderFromIntegerValue() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    Integer i = 13;
    Coder<Integer> coder = registry.getDefaultCoder(i);
    assertEquals(VarIntCoder.of(), coder);
  }

  @Test
  public void testGetDefaultCoderFromNullValue() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    assertEquals(VoidCoder.of(), registry.getDefaultCoder((Void) null));
  }

  @Test
  public void testGetDefaultCoderFromKvValue() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    KV<Integer, String> kv = KV.of(13, "hello");
    Coder<KV<Integer, String>> coder = registry.getDefaultCoder(kv);
    assertEquals(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()),
        coder);
  }

  @Test
  public void testGetDefaultCoderFromKvNullValue() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    KV<Void, Void> kv = KV.of((Void) null, (Void) null);
    assertEquals(KvCoder.of(VoidCoder.of(), VoidCoder.of()),
        registry.getDefaultCoder(kv));
  }

  @Test
  public void testGetDefaultCoderFromNestedKvValue() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    KV<Integer, KV<Long, KV<String, String>>> kv = KV.of(13, KV.of(17L, KV.of("hello", "goodbye")));
    Coder<KV<Integer, KV<Long, KV<String, String>>>> coder = registry.getDefaultCoder(kv);
    assertEquals(
        KvCoder.of(VarIntCoder.of(),
            KvCoder.of(VarLongCoder.of(),
                KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))),
        coder);
  }

  @Test
  public void testTypeCompatibility() throws Exception {
    CoderRegistry.verifyCompatible(BigEndianIntegerCoder.of(), Integer.class);
    CoderRegistry.verifyCompatible(
        ListCoder.of(BigEndianIntegerCoder.of()),
        new TypeDescriptor<List<Integer>>() {}.getType());
  }

  @Test
  public void testIntVersusStringIncompatibility() throws Exception {
    thrown.expect(IncompatibleCoderException.class);
    thrown.expectMessage("not assignable");
    CoderRegistry.verifyCompatible(BigEndianIntegerCoder.of(), String.class);
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
        ListCoder.of(BigEndianIntegerCoder.of()),
        new TypeDescriptor<List<String>>() {}.getType());
  }

  @Test
  public void testDefaultCoderAnnotationGenericRawtype() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    assertEquals(
        registry.getDefaultCoder(MySerializableGeneric.class),
        SerializableCoder.of(MySerializableGeneric.class));
  }

  @Test
  public void testDefaultCoderAnnotationGeneric() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();
    assertEquals(
        registry.getDefaultCoder(new TypeDescriptor<MySerializableGeneric<String>>() {}),
        SerializableCoder.of(MySerializableGeneric.class));
  }

  private static class PTransformOutputingMySerializableGeneric
  extends PTransform<PCollection<String>, PCollection<KV<String, MySerializableGeneric<String>>>> {

    private class OutputDoFn extends DoFn<String, KV<String, MySerializableGeneric<String>>> {
      @ProcessElement
      public void processElement(ProcessContext c) { }
    }

    @Override
    public PCollection<KV<String, MySerializableGeneric<String>>>
    expand(PCollection<String> input) {
      return input.apply(ParDo.of(new OutputDoFn()));
    }
  }

  /**
   * Tests that the error message for a type variable includes a mention of where the
   * type variable was declared.
   */
  @Test
  public void testTypeVariableErrorMessage() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();

    thrown.expect(CannotProvideCoderException.class);
    thrown.expectMessage(allOf(
        containsString("No CoderFactory has been registered"),
        containsString("does not have a @DefaultCoder annotation"),
        containsString("does not implement Serializable")));
    registry.getDefaultCoder(TypeDescriptor.of(
        TestGenericClass.class.getTypeParameters()[0]));
  }

  private static class TestGenericClass<TestGenericT> { }

  @Test
  @SuppressWarnings("rawtypes")
  public void testSerializableTypeVariableDefaultCoder() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();

    TypeDescriptor type = TypeDescriptor.of(
        TestSerializableGenericClass.class.getTypeParameters()[0]);
    assertEquals(registry.getDefaultCoder(type),
        SerializableCoder.of(type));
  }

  private static class TestSerializableGenericClass<TestGenericT extends Serializable> {}

  /**
   * In-context test that assures the functionality tested in
   * {@link #testDefaultCoderAnnotationGeneric} is invoked in the right ways.
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
  extends PTransform<
      PCollection<String>,
      PCollection<KV<String, MySerializableGeneric<T>>>> {

    private class OutputDoFn extends DoFn<String, KV<String, MySerializableGeneric<T>>> {
      @ProcessElement
      public void processElement(ProcessContext c) { }
    }

    @Override
    public PCollection<KV<String, MySerializableGeneric<T>>>
    expand(PCollection<String> input) {
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

  private static class MyGenericClass<FooT, BazT> { }

  private static class MyValue { }

  private static class MyValueCoder extends CustomCoder<MyValue> {

    private static final MyValueCoder INSTANCE = new MyValueCoder();
    private static final TypeDescriptor<MyValue> TYPE_DESCRIPTOR = TypeDescriptor.of(MyValue.class);

    public static MyValueCoder of() {
      return INSTANCE;
    }

    @SuppressWarnings("unused")
    public static List<Object> getInstanceComponents(
        @SuppressWarnings("unused") MyValue exampleValue) {
      return Arrays.asList();
    }

    @Override
    public void encode(MyValue value, OutputStream outStream, Context context)
        throws CoderException, IOException {
    }

    @Override
    public MyValue decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      return new MyValue();
    }

    @Override
    public void verifyDeterministic() { }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public Object structuralValue(MyValue value) {
      return value;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(MyValue value, Context context) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(
        MyValue value, ElementByteSizeObserver observer, Context context)
        throws Exception {
      observer.update(0L);
    }

    @Override
    public TypeDescriptor<MyValue> getEncodedTypeDescriptor() {
      return TYPE_DESCRIPTOR;
    }
  }

  private static class UnknownType { }

  @DefaultCoder(SerializableCoder.class)
  private static class MySerializableGeneric<T extends Serializable> implements Serializable {
    @SuppressWarnings("unused")
    private T foo;
  }

  @Test
  public void testAutomaticRegistrationOfCoders() throws Exception {
    assertEquals(CoderRegistry.createDefault().getDefaultCoder(MyValue.class), MyValueCoder.of());
  }

  /**
   * A {@link CoderRegistrar} to demonstrate default {@link Coder} registration.
   */
  @AutoService(CoderRegistrar.class)
  public static class RegisteredTestCoderRegistrar implements CoderRegistrar {
    @Override
    public Map<Class<?>, CoderFactory> getCoderFactoriesToUseForClasses() {
      return ImmutableMap.<Class<?>, CoderFactory>of(
          MyValue.class, CoderFactories.forCoder(MyValueCoder.of()));
    }
  }
}
