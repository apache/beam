/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.reflect.TypeToken;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for CoderRegistry.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class CoderRegistryTest {

  public static CoderRegistry getStandardRegistry() {
    CoderRegistry registry = new CoderRegistry();
    registry.registerStandardCoders();
    return registry;
  }

  @Test
  public void testRegisterInstantiatedGenericCoder() {
    class MyValueList extends ArrayList<MyValue> { }

    CoderRegistry registry = new CoderRegistry();
    registry.registerCoder(MyValueList.class, ListCoder.of(MyValueCoder.of()));
    assertEquals(registry.getDefaultCoder(MyValueList.class), ListCoder.of(MyValueCoder.of()));
  }

  @Test
  public void testSimpleDefaultCoder() {
    CoderRegistry registry = getStandardRegistry();
    assertEquals(StringUtf8Coder.of(), registry.getDefaultCoder(String.class));
    assertEquals(null, registry.getDefaultCoder(UnknownType.class));
  }

  @Test
  public void testTemplateDefaultCoder() {
    CoderRegistry registry = getStandardRegistry();
    TypeToken<List<Integer>> listToken = new TypeToken<List<Integer>>() {};
    assertEquals(ListCoder.of(VarIntCoder.of()),
                 registry.getDefaultCoder(listToken));

    registry.registerCoder(MyValue.class, MyValueCoder.class);
    TypeToken<KV<String, List<MyValue>>> kvToken =
        new TypeToken<KV<String, List<MyValue>>>() {};
    assertEquals(KvCoder.of(StringUtf8Coder.of(),
                            ListCoder.of(MyValueCoder.of())),
                 registry.getDefaultCoder(kvToken));

    TypeToken<List<UnknownType>> listUnknownToken =
        new TypeToken<List<UnknownType>>() {};
    assertEquals(null, registry.getDefaultCoder(listUnknownToken));
  }

  @Test
  public void testTemplateInference() {
    CoderRegistry registry = getStandardRegistry();
    MyTemplateClass<MyValue, List<MyValue>> instance =
        new MyTemplateClass<MyValue, List<MyValue>>() {};
    Coder<List<MyValue>> expected = ListCoder.of(MyValueCoder.of());

    // The map method operates on parameter names.
    Map<String, Coder<?>> coderMap = registry.getDefaultCoders(
        instance.getClass(),
        MyTemplateClass.class,
        Collections.singletonMap("A", MyValueCoder.of()));
    assertEquals(expected, coderMap.get("B"));

    // The array interface operates on position.
    Coder<?>[] coders = registry.getDefaultCoders(
        instance.getClass(),
        MyTemplateClass.class,
        new Coder<?>[] { MyValueCoder.of(), null });
    assertEquals(expected, coders[1]);

    // The "last argument" coder handles a common case.
    Coder<List<MyValueCoder>> actual = registry.getDefaultCoder(
        instance.getClass(),
        MyTemplateClass.class,
        MyValueCoder.of());
    assertEquals(expected, actual);

    try {
      registry.getDefaultCoder(
          instance.getClass(),
          MyTemplateClass.class,
          BigEndianIntegerCoder.of());
      fail("should have failed");
    } catch (IllegalArgumentException exn) {
      assertEquals("Cannot encode elements of type class "
          + "com.google.cloud.dataflow.sdk.coders.CoderRegistryTest$MyValue "
          + "with BigEndianIntegerCoder", exn.getMessage());
    }
  }

  @Test
  public void testGetDefaultCoderFromIntegerValue() {
    CoderRegistry registry = getStandardRegistry();
    Integer i = 13;
    Coder<Integer> coder = registry.getDefaultCoder(i);
    assertEquals(VarIntCoder.of(), coder);
  }

  @Test
  public void testGetDefaultCoderFromKvValue() {
    CoderRegistry registry = getStandardRegistry();
    KV<Integer, String> kv = KV.of(13, "hello");
    Coder<KV<Integer, String>> coder = registry.getDefaultCoder(kv);
    assertEquals(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()),
        coder);
  }

  @Test
  public void testGetDefaultCoderFromNestedKvValue() {
    CoderRegistry registry = getStandardRegistry();
    KV<Integer, KV<Long, KV<String, String>>> kv = KV.of(13, KV.of(17L, KV.of("hello", "goodbye")));
    Coder<KV<Integer, KV<Long, KV<String, String>>>> coder = registry.getDefaultCoder(kv);
    assertEquals(
        KvCoder.of(VarIntCoder.of(),
            KvCoder.of(VarLongCoder.of(),
                KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))),
        coder);
  }

  @Test
  public void testTypeCompatibility() {
    assertTrue(CoderRegistry.isCompatible(
        BigEndianIntegerCoder.of(), Integer.class));
    assertFalse(CoderRegistry.isCompatible(
        BigEndianIntegerCoder.of(), String.class));

    assertFalse(CoderRegistry.isCompatible(
        ListCoder.of(BigEndianIntegerCoder.of()), Integer.class));
    assertTrue(CoderRegistry.isCompatible(
        ListCoder.of(BigEndianIntegerCoder.of()),
        new TypeToken<List<Integer>>() {}.getType()));
    assertFalse(CoderRegistry.isCompatible(
        ListCoder.of(BigEndianIntegerCoder.of()),
        new TypeToken<List<String>>() {}.getType()));
  }

  static class MyTemplateClass<A, B> { }

  static class MyValue { }

  static class MyValueCoder implements Coder<MyValue> {

    private static final MyValueCoder INSTANCE = new MyValueCoder();

    public static MyValueCoder of() {
      return INSTANCE;
    }

    public static List<Object> getInstanceComponents(MyValue exampleValue) {
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
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public CloudObject asCloudObject() {
      return null;
    }

    @Override
    public boolean isDeterministic() { return true; }

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
  }

  static class UnknownType { }
}
