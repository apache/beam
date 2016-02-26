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

package com.google.cloud.dataflow.sdk.transforms;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.transforms.DoFnWithContext.Context;
import com.google.cloud.dataflow.sdk.transforms.DoFnWithContext.ExtraContextFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFnWithContext.ProcessContext;
import com.google.cloud.dataflow.sdk.transforms.DoFnWithContext.ProcessElement;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Method;

/**
 * Tests for {@link DoFnReflector}.
 */
@RunWith(JUnit4.class)
public class DoFnReflectorTest {

  private boolean wasProcessElementInvoked = false;
  private boolean wasStartBundleInvoked = false;
  private boolean wasFinishBundleInvoked = false;

  private DoFnWithContext<String, String> fn;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Mock
  private DoFnWithContext<String, String>.ProcessContext mockContext;
  @Mock
  private BoundedWindow mockWindow;
  @Mock
  private WindowingInternals<String, String> mockWindowingInternals;

  private ExtraContextFactory<String, String> extraContextFactory;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    this.extraContextFactory = new ExtraContextFactory<String, String>() {
      @Override
      public BoundedWindow window() {
        return mockWindow;
      }

      @Override
      public WindowingInternals<String, String> windowingInternals() {
        return mockWindowingInternals;
      }
    };
  }

  private DoFnReflector underTest(DoFnWithContext<String, String> fn) {
    this.fn = fn;
    return DoFnReflector.of(fn.getClass());
  }

  private void checkInvokeProcessElementWorks(DoFnReflector r) throws Exception {
    assertFalse(wasProcessElementInvoked);
    r.invokeProcessElement(fn, mockContext, extraContextFactory);
    assertTrue(wasProcessElementInvoked);
  }

  private void checkInvokeStartBundleWorks(DoFnReflector r) throws Exception {
    assertFalse(wasStartBundleInvoked);
    r.invokeStartBundle(fn, mockContext, extraContextFactory);
    assertTrue(wasStartBundleInvoked);
  }

  private void checkInvokeFinishBundleWorks(DoFnReflector r) throws Exception {
    assertFalse(wasFinishBundleInvoked);
    r.invokeFinishBundle(fn, mockContext, extraContextFactory);
    assertTrue(wasFinishBundleInvoked);
  }

  @Test
  public void testDoFnWithNoExtraContext() throws Exception {
    DoFnReflector reflector = underTest(new DoFnWithContext<String, String>() {

      @ProcessElement
      public void processElement(ProcessContext c)
          throws Exception {
        wasProcessElementInvoked = true;
        assertSame(c, mockContext);
      }
    });

    assertFalse(reflector.usesSingleWindow());

    checkInvokeProcessElementWorks(reflector);
  }

  interface InterfaceWithProcessElement {
    @ProcessElement
    void processElement(DoFnWithContext<String, String>.ProcessContext c);
  }

  interface LayersOfInterfaces extends InterfaceWithProcessElement {}

  private class IdentityUsingInterfaceWithProcessElement
      extends DoFnWithContext<String, String>
      implements LayersOfInterfaces {

    @Override
    public void processElement(DoFnWithContext<String, String>.ProcessContext c) {
      wasProcessElementInvoked = true;
      assertSame(c, mockContext);
    }
  }

  @Test
  public void testDoFnWithProcessElementInterface() throws Exception {
    DoFnReflector reflector = underTest(new IdentityUsingInterfaceWithProcessElement());
    assertFalse(reflector.usesSingleWindow());
    checkInvokeProcessElementWorks(reflector);
  }

  private class IdentityParent extends DoFnWithContext<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {
      wasProcessElementInvoked = true;
      assertSame(c, mockContext);
    }
  }

  private class IdentityChild extends IdentityParent {}

  @Test
  public void testDoFnWithMethodInSuperclass() throws Exception {
    DoFnReflector reflector = underTest(new IdentityChild());
    assertFalse(reflector.usesSingleWindow());
    checkInvokeProcessElementWorks(reflector);
  }

  @Test
  public void testDoFnWithWindow() throws Exception {
    DoFnReflector reflector = underTest(new DoFnWithContext<String, String>() {

      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow w)
          throws Exception {
        wasProcessElementInvoked = true;
        assertSame(c, mockContext);
        assertSame(w, mockWindow);
      }
    });

    assertTrue(reflector.usesSingleWindow());

    checkInvokeProcessElementWorks(reflector);
  }

  @Test
  public void testDoFnWithWindowingInternals() throws Exception {
    DoFnReflector reflector = underTest(new DoFnWithContext<String, String>() {

      @ProcessElement
      public void processElement(ProcessContext c, WindowingInternals<String, String> w)
          throws Exception {
        wasProcessElementInvoked = true;
        assertSame(c, mockContext);
        assertSame(w, mockWindowingInternals);
      }
    });

    assertFalse(reflector.usesSingleWindow());

    checkInvokeProcessElementWorks(reflector);
  }

  @Test
  public void testDoFnWithStartBundle() throws Exception {
    DoFnReflector reflector = underTest(new DoFnWithContext<String, String>() {
      @ProcessElement
      public void processElement(@SuppressWarnings("unused") ProcessContext c) {}

      @StartBundle
      public void startBundle(Context c) {
        wasStartBundleInvoked = true;
        assertSame(c, mockContext);
      }

      @FinishBundle
      public void finishBundle(Context c) {
        wasFinishBundleInvoked = true;
        assertSame(c, mockContext);
      }
    });

    checkInvokeStartBundleWorks(reflector);
    checkInvokeFinishBundleWorks(reflector);
  }

  @Test
  public void testNoProcessElement() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("No method annotated with @ProcessElement found");
    thrown.expectMessage(getClass().getName() + "$");
    underTest(new DoFnWithContext<String, String>() {});
  }

  @Test
  public void testMultipleProcessElement() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Found multiple methods annotated with @ProcessElement");
    thrown.expectMessage("foo()");
    thrown.expectMessage("bar()");
    thrown.expectMessage(getClass().getName() + "$");
    underTest(new DoFnWithContext<String, String>() {
      @ProcessElement
      public void foo() {}

      @ProcessElement
      public void bar() {}
    });
  }

  @Test
  public void testMultipleStartBundleElement() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Found multiple methods annotated with @StartBundle");
    thrown.expectMessage("bar()");
    thrown.expectMessage("baz()");
    thrown.expectMessage(getClass().getName() + "$");
    underTest(new DoFnWithContext<String, String>() {
      @ProcessElement
      public void foo() {}

      @StartBundle
      public void bar() {}

      @StartBundle
      public void baz() {}
    });
  }

  @Test
  public void testMultipleFinishBundleElement() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Found multiple methods annotated with @FinishBundle");
    thrown.expectMessage("bar()");
    thrown.expectMessage("baz()");
    thrown.expectMessage(getClass().getName() + "$");
    underTest(new DoFnWithContext<String, String>() {
      @ProcessElement
      public void foo() {}

      @FinishBundle
      public void bar() {}

      @FinishBundle
      public void baz() {}
    });
  }

  @Test
  public void testPrivateProcessElement() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("process() must be public");
    thrown.expectMessage(getClass().getName() + "$");
    underTest(new DoFnWithContext<String, String>() {
      @ProcessElement
      private void process() {}
    });
  }

  @Test
  public void testPrivateStartBundle() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("startBundle() must be public");
    thrown.expectMessage(getClass().getName() + "$");
    underTest(new DoFnWithContext<String, String>() {
      @ProcessElement
      public void processElement() {}

      @StartBundle
      void startBundle() {}
    });
  }

  @Test
  public void testPrivateFinishBundle() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("finishBundle() must be public");
    thrown.expectMessage(getClass().getName() + "$");
    underTest(new DoFnWithContext<String, String>() {
      @ProcessElement
      public void processElement() {}

      @FinishBundle
      void finishBundle() {}
    });
  }

  @SuppressWarnings({"unused", "rawtypes"})
  private void missingProcessContext() {}

  @Test
  public void testMissingProcessContext() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(getClass().getName()
        + "#missingProcessContext() must take a ProcessContext as its first argument");

    DoFnReflector.verifyProcessMethodArguments(
        getClass().getDeclaredMethod("missingProcessContext"));
  }

  @SuppressWarnings({"unused", "rawtypes"})
  private void badProcessContext(String s) {}

  @Test
  public void testBadProcessContextType() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(getClass().getName()
        + "#badProcessContext(String) must take a ProcessContext as its first argument");

    DoFnReflector.verifyProcessMethodArguments(
        getClass().getDeclaredMethod("badProcessContext", String.class));
  }

  @SuppressWarnings({"unused", "rawtypes"})
  private void badExtraContext(DoFnWithContext<Integer, String>.Context c, int n) {}

  @Test
  public void testBadExtraContext() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "int is not a valid context parameter for method "
        + getClass().getName() + "#badExtraContext(Context, int). Should be one of [");

    DoFnReflector.verifyBundleMethodArguments(
        getClass().getDeclaredMethod("badExtraContext", Context.class, int.class));
  }

  @SuppressWarnings({"unused", "rawtypes"})
  private void badExtraProcessContext(
      DoFnWithContext<Integer, String>.ProcessContext c, Integer n) {}

  @Test
  public void testBadExtraProcessContextType() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Integer is not a valid context parameter for method "
        + getClass().getName() + "#badExtraProcessContext(ProcessContext, Integer)"
        + ". Should be one of [BoundedWindow, WindowingInternals<Integer, String>]");

    DoFnReflector.verifyProcessMethodArguments(
        getClass().getDeclaredMethod("badExtraProcessContext",
            ProcessContext.class, Integer.class));
  }

  @SuppressWarnings("unused")
  private int badReturnType() {
    return 0;
  }

  @Test
  public void testBadReturnType() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(getClass().getName() + "#badReturnType() must have a void return type");

    DoFnReflector.verifyProcessMethodArguments(getClass().getDeclaredMethod("badReturnType"));
  }

  @SuppressWarnings("unused")
  private void goodGenerics(DoFnWithContext<Integer, String>.ProcessContext c,
      WindowingInternals<Integer, String> i1) {}

  @Test
  public void testValidGenerics() throws Exception {
    Method method = getClass().getDeclaredMethod("goodGenerics",
        DoFnWithContext.ProcessContext.class, WindowingInternals.class);
    DoFnReflector.verifyProcessMethodArguments(method);
  }

  @SuppressWarnings("unused")
  private void goodWildcards(DoFnWithContext<Integer, String>.ProcessContext c,
      WindowingInternals<?, ?> i1) {}

  @Test
  public void testGoodWildcards() throws Exception {
    Method method = getClass().getDeclaredMethod("goodWildcards",
        DoFnWithContext.ProcessContext.class, WindowingInternals.class);
    DoFnReflector.verifyProcessMethodArguments(method);
  }

  @SuppressWarnings("unused")
  private void goodBoundedWildcards(DoFnWithContext<Integer, String>.ProcessContext c,
      WindowingInternals<? super Integer, ? super String> i1) {}

  @Test
  public void testGoodBoundedWildcards() throws Exception {
    Method method = getClass().getDeclaredMethod("goodBoundedWildcards",
        DoFnWithContext.ProcessContext.class, WindowingInternals.class);
    DoFnReflector.verifyProcessMethodArguments(method);
  }

  @SuppressWarnings("unused")
  private <InputT, OutputT> void goodTypeVariables(
      DoFnWithContext<InputT, OutputT>.ProcessContext c,
      WindowingInternals<InputT, OutputT> i1) {}

  @Test
  public void testGoodTypeVariables() throws Exception {
    Method method = getClass().getDeclaredMethod("goodTypeVariables",
        DoFnWithContext.ProcessContext.class, WindowingInternals.class);
    DoFnReflector.verifyProcessMethodArguments(method);
  }

  @SuppressWarnings("unused")
  private void badGenericTwoArgs(DoFnWithContext<Integer, String>.ProcessContext c,
      WindowingInternals<Integer, Integer> i1) {}

  @Test
  public void testBadGenericsTwoArgs() throws Exception {
    Method method = getClass().getDeclaredMethod("badGenericTwoArgs",
        DoFnWithContext.ProcessContext.class, WindowingInternals.class);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Incompatible generics in context parameter "
        + "WindowingInternals<Integer, Integer> "
        + "for method " + getClass().getName()
        + "#badGenericTwoArgs(ProcessContext, WindowingInternals). Should be "
        + "WindowingInternals<Integer, String>");

    DoFnReflector.verifyProcessMethodArguments(method);
  }

  @SuppressWarnings("unused")
  private void badGenericWildCards(DoFnWithContext<Integer, String>.ProcessContext c,
      WindowingInternals<Integer, ? super Integer> i1) {}

  @Test
  public void testBadGenericWildCards() throws Exception {
    Method method = getClass().getDeclaredMethod("badGenericWildCards",
        DoFnWithContext.ProcessContext.class, WindowingInternals.class);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Incompatible generics in context parameter "
        + "WindowingInternals<Integer, ? super Integer> for method "
        + getClass().getName()
        + "#badGenericWildCards(ProcessContext, WindowingInternals). Should be "
        + "WindowingInternals<Integer, String>");

    DoFnReflector.verifyProcessMethodArguments(method);
  }

  @SuppressWarnings("unused")
  private <InputT, OutputT> void badTypeVariables(DoFnWithContext<InputT, OutputT>.ProcessContext c,
      WindowingInternals<InputT, InputT> i1) {}

  @Test
  public void testBadTypeVariables() throws Exception {
    Method method = getClass().getDeclaredMethod("badTypeVariables",
        DoFnWithContext.ProcessContext.class, WindowingInternals.class);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Incompatible generics in context parameter "
        + "WindowingInternals<InputT, InputT> for method " + getClass().getName()
        + "#badTypeVariables(ProcessContext, WindowingInternals). Should be "
        + "WindowingInternals<InputT, OutputT>");

    DoFnReflector.verifyProcessMethodArguments(method);
  }
}
