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
package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link UserCodeException} functionality. */
@RunWith(JUnit4.class)
public class UserCodeExceptionTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void existingUserCodeExceptionsNotWrapped() {
    UserCodeException existing = UserCodeException.wrap(new IOException());
    UserCodeException wrapped = UserCodeException.wrap(existing);

    assertEquals(existing, wrapped);
  }

  @Test
  public void testCauseIsSet() {
    thrown.expectCause(isA(IOException.class));
    throwUserCodeException();
  }

  @Test
  public void testStackTraceIsTruncatedToUserCode() {
    thrown.expectCause(hasBottomStackFrame(method("userCode")));
    throwUserCodeException();
  }

  @Test
  public void testStackTraceIsTruncatedProperlyFromHelperMethod() {
    thrown.expectCause(hasBottomStackFrame(method("userCode")));
    throwUserCodeExceptionFromHelper();
  }

  @Test
  public void testWrapIfOnlyWrapsWhenTrue() {
    IOException cause = new IOException();
    RuntimeException wrapped = UserCodeException.wrapIf(true, cause);

    assertThat(wrapped, is(instanceOf(UserCodeException.class)));
  }

  @Test
  public void testWrapIfReturnsRuntimeExceptionWhenFalse() {
    IOException cause = new IOException();
    RuntimeException wrapped = UserCodeException.wrapIf(false, cause);

    assertThat(wrapped, is(not(instanceOf(UserCodeException.class))));
    assertEquals(cause, wrapped.getCause());
  }

  @Test
  public void testWrapIfReturnsSourceRuntimeExceptionWhenFalse() {
    RuntimeException runtimeException = new RuntimeException("oh noes!");
    RuntimeException wrapped = UserCodeException.wrapIf(false, runtimeException);

    assertEquals(runtimeException, wrapped);
  }

  @Test
  public void robustAgainstEmptyStackTrace() {
    RuntimeException runtimeException = new RuntimeException("empty stack");
    runtimeException.setStackTrace(new StackTraceElement[0]);
    RuntimeException wrapped = UserCodeException.wrap(runtimeException);
    assertEquals(runtimeException, wrapped.getCause());
  }

  private void throwUserCodeException() {
    try {
      userCode();
    } catch (Exception ex) {
      throw UserCodeException.wrap(ex);
    }
  }

  private void throwUserCodeExceptionFromHelper() {
    try {
      userCode();
    } catch (Exception ex) {
      throw wrap(ex);
    }
  }

  private UserCodeException wrap(Throwable t) {
    throw UserCodeException.wrap(t);
  }

  private void userCode() throws IOException {
    userCode2();
  }

  private void userCode2() throws IOException {
    userCode3();
  }

  private void userCode3() throws IOException {
    IOException ex = new IOException("User processing error!");
    throw ex;
  }

  private static ThrowableBottomStackFrameMethodMatcher hasBottomStackFrame(
      Matcher<StackTraceElement> frameMatcher) {
    return new ThrowableBottomStackFrameMethodMatcher(frameMatcher);
  }

  private static StackFrameMethodMatcher method(String methodName) {
    return new StackFrameMethodMatcher(is(methodName));
  }

  static class ThrowableBottomStackFrameMethodMatcher
      extends FeatureMatcher<Throwable, StackTraceElement> {

    public ThrowableBottomStackFrameMethodMatcher(Matcher<StackTraceElement> subMatcher) {
      super(subMatcher, "Throwable with bottom stack frame:", "stack frame");
    }

    @Override
    protected StackTraceElement featureValueOf(Throwable actual) {
      StackTraceElement[] stackTrace = actual.getStackTrace();
      return stackTrace[stackTrace.length - 1];
    }
  }

  static class StackFrameMethodMatcher extends TypeSafeMatcher<StackTraceElement> {

    private Matcher<String> methodNameMatcher;

    public StackFrameMethodMatcher(Matcher<String> methodNameMatcher) {
      this.methodNameMatcher = methodNameMatcher;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("stack frame where method name ");
      methodNameMatcher.describeTo(description);
    }

    @Override
    protected boolean matchesSafely(StackTraceElement item) {
      return methodNameMatcher.matches(item.getMethodName());
    }
  }
}
