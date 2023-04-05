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
package org.apache.beam.sdk.extensions.sql.impl;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.beam.sdk.extensions.sql.udf.ScalarFn;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ScalarFnReflector}. */
@RunWith(JUnit4.class)
public class ScalarFnReflectorTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  @SuppressWarnings("nullness") // If result is null, test will fail as expected.
  public void testGetApplyMethod() throws InvocationTargetException, IllegalAccessException {
    IncrementFn incrementFn = new IncrementFn();
    Method method = ScalarFnReflector.getApplyMethod(incrementFn);
    @Nullable Object result = method.invoke(incrementFn, Long.valueOf(24L));
    assertEquals(Long.valueOf(25L), result);
  }

  @Test
  @SuppressWarnings("nullness") // If result is null, test will fail as expected.
  public void testGetApplyMethodOverride()
      throws InvocationTargetException, IllegalAccessException {
    IncrementFnChild incrementFn = new IncrementFnChild();
    Method method = ScalarFnReflector.getApplyMethod(incrementFn);
    @Nullable Object result = method.invoke(incrementFn, Long.valueOf(24L));
    assertEquals(Long.valueOf(26L), result);
  }

  @Test
  @SuppressWarnings("nullness") // If result is null, test will fail as expected.
  public void testGetApplyMethodStatic() throws InvocationTargetException, IllegalAccessException {
    Method method = ScalarFnReflector.getApplyMethod(new IncrementFnWithStaticMethod());
    @Nullable Object result = method.invoke(null, Long.valueOf(24L));
    assertEquals(Long.valueOf(25L), result);
  }

  @Test
  public void testDifferentMethodNameThrowsIllegalArgumentException() {
    thrown.expect(instanceOf(IllegalArgumentException.class));
    thrown.expectMessage("Found multiple methods annotated with @ApplyMethod.");
    ScalarFnReflector.getApplyMethod(new IncrementFnDifferentMethodName());
  }

  @Test
  public void testDifferentMethodSignatureThrowsIllegalArgumentException() {
    thrown.expect(instanceOf(IllegalArgumentException.class));
    thrown.expectMessage("Found multiple methods annotated with @ApplyMethod.");
    ScalarFnReflector.getApplyMethod(new IncrementFnDifferentSignature());
  }

  @Test
  public void testMissingAnnotationThrowsIllegalArgumentException() {
    thrown.expect(instanceOf(IllegalArgumentException.class));
    thrown.expectMessage("No method annotated with @ApplyMethod found in class");
    ScalarFnReflector.getApplyMethod(new IncrementFnMissingAnnotation());
  }

  @Test
  public void testNonPublicMethodThrowsIllegalArgumentException() {
    thrown.expect(instanceOf(IllegalArgumentException.class));
    thrown.expectMessage("not public");
    ScalarFnReflector.getApplyMethod(new IncrementFnWithProtectedMethod());
  }

  static class IncrementFn extends ScalarFn {
    @ApplyMethod
    public Long increment(Long i) {
      return i + 1;
    }
  }

  static class IncrementFnChild extends IncrementFn {
    @ApplyMethod
    @Override
    public Long increment(Long i) {
      return i + 2;
    }
  }

  static class IncrementFnWithStaticMethod extends ScalarFn {
    @ApplyMethod
    public static Long increment(Long i) {
      return i + 1;
    }
  }

  static class IncrementFnDifferentMethodName extends IncrementFn {
    @ApplyMethod
    public Long differentMethod(Long i) {
      return i + 2;
    }
  }

  static class IncrementFnDifferentSignature extends IncrementFn {
    @ApplyMethod
    public Long increment(String s) {
      return 0L;
    }
  }

  static class IncrementFnMissingAnnotation extends ScalarFn {
    public Long increment(Long i) {
      return i + 1;
    }
  }

  static class IncrementFnWithProtectedMethod extends ScalarFn {
    @ApplyMethod
    protected Long increment(Long i) {
      return i + 1;
    }
  }
}
