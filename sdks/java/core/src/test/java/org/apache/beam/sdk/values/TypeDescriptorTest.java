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
package org.apache.beam.sdk.values;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Method;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.reflect.TypeToken;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for TypeDescriptor. */
@RunWith(JUnit4.class)
public class TypeDescriptorTest {

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTypeDescriptorOfRawType() throws Exception {
    assertEquals(
        TypeToken.of(String.class).getRawType(), TypeDescriptor.of(String.class).getRawType());
  }

  @Test
  public void testTypeDescriptorImmediate() throws Exception {
    assertEquals(Boolean.class, new TypeDescriptor<Boolean>() {}.getRawType());
    assertEquals(Double.class, new TypeDescriptor<Double>() {}.getRawType());
    assertEquals(Float.class, new TypeDescriptor<Float>() {}.getRawType());
    assertEquals(Integer.class, new TypeDescriptor<Integer>() {}.getRawType());
    assertEquals(Long.class, new TypeDescriptor<Long>() {}.getRawType());
    assertEquals(Short.class, new TypeDescriptor<Short>() {}.getRawType());
    assertEquals(String.class, new TypeDescriptor<String>() {}.getRawType());
  }

  @Test
  public void testTypeDescriptorGeneric() throws Exception {
    TypeDescriptor<List<String>> descriptor = new TypeDescriptor<List<String>>() {};
    TypeToken<List<String>> token = new TypeToken<List<String>>() {};
    assertEquals(token.getType(), descriptor.getType());
  }

  private static class TypeRememberer<T> {
    public final TypeDescriptor<T> descriptorByClass;
    public final TypeDescriptor<T> descriptorByInstance;

    public TypeRememberer() {
      descriptorByClass = new TypeDescriptor<T>(getClass()) {};
      descriptorByInstance = new TypeDescriptor<T>(this) {};
    }
  }

  @Test
  public void testTypeDescriptorNested() throws Exception {
    TypeRememberer<String> rememberer = new TypeRememberer<String>() {};
    assertEquals(new TypeToken<String>() {}.getType(), rememberer.descriptorByClass.getType());
    assertEquals(new TypeToken<String>() {}.getType(), rememberer.descriptorByInstance.getType());

    TypeRememberer<List<String>> genericRememberer = new TypeRememberer<List<String>>() {};
    assertEquals(
        new TypeToken<List<String>>() {}.getType(), genericRememberer.descriptorByClass.getType());
    assertEquals(
        new TypeToken<List<String>>() {}.getType(),
        genericRememberer.descriptorByInstance.getType());
  }

  private static class Id<T> {
    @SuppressWarnings("unused") // used via reflection
    public T identity(T thingie) {
      return thingie;
    }
  }

  @Test
  public void testGetArgumentTypes() throws Exception {
    Method identity = Id.class.getDeclaredMethod("identity", Object.class);

    TypeToken<Id<String>> token = new TypeToken<Id<String>>() {};
    TypeDescriptor<Id<String>> descriptor = new TypeDescriptor<Id<String>>() {};
    assertEquals(
        token.method(identity).getParameters().get(0).getType().getType(),
        descriptor.getArgumentTypes(identity).get(0).getType());

    TypeToken<Id<List<String>>> genericToken = new TypeToken<Id<List<String>>>() {};
    TypeDescriptor<Id<List<String>>> genericDescriptor = new TypeDescriptor<Id<List<String>>>() {};
    assertEquals(
        genericToken.method(identity).getParameters().get(0).getType().getType(),
        genericDescriptor.getArgumentTypes(identity).get(0).getType());
  }

  private static class TypeRemembererer<T1, T2> {
    public TypeDescriptor<T1> descriptor1;
    public TypeDescriptor<T2> descriptor2;

    public TypeRemembererer() {
      descriptor1 = new TypeDescriptor<T1>(getClass()) {};
      descriptor2 = new TypeDescriptor<T2>(getClass()) {};
    }
  }

  @Test
  public void testTypeDescriptorNested2() throws Exception {
    TypeRemembererer<String, Integer> remembererer = new TypeRemembererer<String, Integer>() {};
    assertEquals(new TypeToken<String>() {}.getType(), remembererer.descriptor1.getType());
    assertEquals(new TypeToken<Integer>() {}.getType(), remembererer.descriptor2.getType());

    TypeRemembererer<List<String>, Set<Integer>> genericRemembererer =
        new TypeRemembererer<List<String>, Set<Integer>>() {};
    assertEquals(
        new TypeToken<List<String>>() {}.getType(), genericRemembererer.descriptor1.getType());
    assertEquals(
        new TypeToken<Set<Integer>>() {}.getType(), genericRemembererer.descriptor2.getType());
  }

  private static class GenericClass<BizzleT> {}

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

  private static class GenericMaker<T> {
    public TypeRememberer<List<T>> getRememberer() {
      return new TypeRememberer<List<T>>() {};
    }
  }

  private static class GenericMaker2<T> {
    public GenericMaker<Set<T>> getGenericMaker() {
      return new GenericMaker<Set<T>>() {};
    }
  }

  @Test
  public void testEnclosing() throws Exception {
    TypeRememberer<List<String>> rememberer = new GenericMaker<String>() {}.getRememberer();
    assertEquals(
        new TypeToken<List<String>>() {}.getType(), rememberer.descriptorByInstance.getType());

    // descriptorByClass *is not* able to find the type of T because it comes from the enclosing
    // instance of GenericMaker.
    // assertEquals(new TypeToken<List<T>>() {}.getType(), rememberer.descriptorByClass.getType());
  }

  @Test
  public void testEnclosing2() throws Exception {
    // If we don't override, the best we can get is List<Set<T>>
    // TypeRememberer<List<Set<String>>> rememberer =
    //    new GenericMaker2<String>(){}.getGenericMaker().getRememberer();
    // assertNotEquals(
    //    new TypeToken<List<Set<String>>>() {}.getType(),
    //    rememberer.descriptorByInstance.getType());

    // If we've overridden the getGenericMaker we can determine the types.
    TypeRememberer<List<Set<String>>> rememberer =
        new GenericMaker2<String>() {
          @Override
          public GenericMaker<Set<String>> getGenericMaker() {
            return new GenericMaker<Set<String>>() {};
          }
        }.getGenericMaker().getRememberer();
    assertEquals(
        new TypeToken<List<Set<String>>>() {}.getType(), rememberer.descriptorByInstance.getType());
  }

  @Test
  public void testWhere() throws Exception {
    useWhereMethodToDefineTypeParam(new TypeDescriptor<String>() {});
  }

  private <T> void useWhereMethodToDefineTypeParam(TypeDescriptor<T> parameterType) {
    TypeDescriptor<Set<T>> typeDescriptor =
        new TypeDescriptor<Set<T>>() {}.where(new TypeParameter<T>() {}, parameterType);
    assertEquals(new TypeToken<Set<String>>() {}.getType(), typeDescriptor.getType());
  }
}
