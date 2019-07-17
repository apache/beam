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
package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DoFnLifecycleManagers}. */
@RunWith(JUnit4.class)
public class DoFnLifecycleManagersTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void removeAllWhenManagersThrowSuppressesAndThrows() throws Exception {
    DoFnLifecycleManager first = DoFnLifecycleManager.of(new ThrowsInCleanupFn("foo"));
    DoFnLifecycleManager second = DoFnLifecycleManager.of(new ThrowsInCleanupFn("bar"));
    DoFnLifecycleManager third = DoFnLifecycleManager.of(new ThrowsInCleanupFn("baz"));
    first.get();
    second.get();
    third.get();

    final Collection<Matcher<? super Throwable>> suppressions = new ArrayList<>();
    suppressions.add(
        allOf(
            instanceOf(UserCodeException.class),
            new CausedByMatcher(new ThrowableMessageMatcher("foo"))));
    suppressions.add(
        allOf(
            instanceOf(UserCodeException.class),
            new CausedByMatcher(new ThrowableMessageMatcher("bar"))));
    suppressions.add(
        allOf(
            instanceOf(UserCodeException.class),
            new CausedByMatcher(new ThrowableMessageMatcher("baz"))));

    thrown.expect(
        new BaseMatcher<Exception>() {
          @Override
          public void describeTo(Description description) {
            description
                .appendText("Exception suppressing ")
                .appendList("[", ", ", "]", suppressions);
          }

          @Override
          public boolean matches(Object item) {
            if (!(item instanceof Exception)) {
              return false;
            }
            Exception that = (Exception) item;
            return Matchers.containsInAnyOrder(suppressions)
                .matches(ImmutableList.copyOf(that.getSuppressed()));
          }
        });

    DoFnLifecycleManagers.removeAllFromManagers(ImmutableList.of(first, second, third));
  }

  @Test
  public void whenManagersSucceedSucceeds() throws Exception {
    DoFnLifecycleManager first = DoFnLifecycleManager.of(new EmptyFn());
    DoFnLifecycleManager second = DoFnLifecycleManager.of(new EmptyFn());
    DoFnLifecycleManager third = DoFnLifecycleManager.of(new EmptyFn());
    first.get();
    second.get();
    third.get();

    DoFnLifecycleManagers.removeAllFromManagers(ImmutableList.of(first, second, third));
  }

  private static class ThrowsInCleanupFn extends DoFn<Object, Object> {
    private final String message;

    private ThrowsInCleanupFn(String message) {
      this.message = message;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {}

    @Teardown
    public void teardown() throws Exception {
      throw new Exception(message);
    }
  }

  private static class ThrowableMessageMatcher extends BaseMatcher<Throwable> {
    private final Matcher<String> messageMatcher;

    public ThrowableMessageMatcher(String message) {
      this.messageMatcher = equalTo(message);
    }

    @Override
    public boolean matches(Object item) {
      if (!(item instanceof Throwable)) {
        return false;
      }
      Throwable that = (Throwable) item;
      return messageMatcher.matches(that.getMessage());
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a throwable with a message ").appendDescriptionOf(messageMatcher);
    }
  }

  private static class CausedByMatcher extends BaseMatcher<Throwable> {
    private final Matcher<Throwable> causeMatcher;

    public CausedByMatcher(Matcher<Throwable> causeMatcher) {
      this.causeMatcher = causeMatcher;
    }

    @Override
    public boolean matches(Object item) {
      if (!(item instanceof UserCodeException)) {
        return false;
      }
      UserCodeException that = (UserCodeException) item;
      return causeMatcher.matches(that.getCause());
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a throwable with a cause ").appendDescriptionOf(causeMatcher);
    }
  }

  private static class EmptyFn extends DoFn<Object, Object> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {}
  }
}
