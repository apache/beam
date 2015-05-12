/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.values;

import static org.junit.Assert.assertEquals;

import com.google.common.reflect.TypeToken;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.lang.reflect.Method;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.Set;

/**
 * Tests for TypeDescriptor.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class TypeDescriptorTest {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTypeDescriptorOfRawType() throws Exception {
    assertEquals(
        TypeToken.of(String.class).getRawType(),
        TypeDescriptor.of(String.class).getRawType());
  }

  @Test
  public void testTypeDescriptorImmediate() throws Exception {
    TypeDescriptor<String> descriptor = new TypeDescriptor<String>(){};
    assertEquals(String.class, descriptor.getRawType());
  }

  @Test
  public void testTypeDescriptorGeneric() throws Exception {
    TypeDescriptor<List<String>> descriptor = new TypeDescriptor<List<String>>(){};
    TypeToken<List<String>> token = new TypeToken<List<String>>(){};
    assertEquals(token.getType(), descriptor.getType());
  }

  private static class TypeRememberer<T> {
    public TypeToken<T> token;
    public TypeDescriptor<T> descriptor;

    public TypeRememberer() {
      token = new TypeToken<T>(getClass()){};
      descriptor = new TypeDescriptor<T>(getClass()){};
    }
  }

  @Test
  public void testTypeDescriptorNested() throws Exception {
    TypeRememberer<String> rememberer = new TypeRememberer<String>(){};
    assertEquals(rememberer.token.getType(), rememberer.descriptor.getType());

    TypeRememberer<List<String>> genericRememberer = new TypeRememberer<List<String>>(){};
    assertEquals(genericRememberer.token.getType(), genericRememberer.descriptor.getType());
  }

  private static class Id<T> {
    public T identity(T thingie) {
      return thingie;
    }
  }


  @Test
  public void testGetArgumentTypes() throws Exception {
    Method identity = Id.class.getDeclaredMethod("identity", Object.class);

    TypeToken<Id<String>> token = new TypeToken <Id<String>>(){};
    TypeDescriptor<Id<String>> descriptor = new TypeDescriptor <Id<String>>(){};
    assertEquals(
        token.method(identity).getParameters().get(0).getType().getType(),
        descriptor.getArgumentTypes(identity).get(0).getType());

    TypeToken<Id<List<String>>> genericToken = new TypeToken <Id<List<String>>>(){};
    TypeDescriptor<Id<List<String>>> genericDescriptor = new TypeDescriptor <Id<List<String>>>(){};
    assertEquals(
        genericToken.method(identity).getParameters().get(0).getType().getType(),
        genericDescriptor.getArgumentTypes(identity).get(0).getType());
  }

  private static class TypeRemembererer<T1, T2> {
    public TypeToken<T1> token1;
    public TypeToken<T2> token2;

    public TypeDescriptor<T1> descriptor1;
    public TypeDescriptor<T2> descriptor2;

    public TypeRemembererer() {
      token1 = new TypeToken<T1>(getClass()){};
      token2 = new TypeToken<T2>(getClass()){};
      descriptor1 = new TypeDescriptor<T1>(getClass()){};
      descriptor2 = new TypeDescriptor<T2>(getClass()){};
    }
  }

  @Test
  public void testTypeDescriptorNested2() throws Exception {
    TypeRemembererer<String, Integer> remembererer = new TypeRemembererer<String, Integer>(){};
    assertEquals(remembererer.token1.getType(), remembererer.descriptor1.getType());
    assertEquals(remembererer.token2.getType(), remembererer.descriptor2.getType());

    TypeRemembererer<List<String>, Set<Integer>> genericRemembererer =
        new TypeRemembererer<List<String>, Set<Integer>>(){};
    assertEquals(genericRemembererer.token1.getType(), genericRemembererer.descriptor1.getType());
    assertEquals(genericRemembererer.token2.getType(), genericRemembererer.descriptor2.getType());
  }

  private static class GenericClass<BizzleT> { }

  @Test
  public void testGetTypeParameterGood() throws Exception {
    @SuppressWarnings("rawtypes")
    TypeVariable<Class<? super GenericClass>> bizzleT =
        TypeDescriptor.of(GenericClass.class).getTypeParameter("BizzleT");
    assertEquals(GenericClass.class.getTypeParameters()[0], bizzleT);
  }

  @Test
  public void testGetTypeParameterBad() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("MerpleT"); // just check that the message gives actionable details
    TypeDescriptor.of(GenericClass.class).getTypeParameter("MerpleT");
  }
}
