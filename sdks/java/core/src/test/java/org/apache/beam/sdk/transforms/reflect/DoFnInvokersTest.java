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
package org.apache.beam.sdk.transforms.reflect;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.UserCodeException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DoFnInvokers}. */
@RunWith(JUnit4.class)
public class DoFnInvokersTest {
  /** A convenience struct holding flags that indicate whether a particular method was invoked. */
  public static class Invocations {
    public boolean wasProcessElementInvoked = false;
    public boolean wasStartBundleInvoked = false;
    public boolean wasFinishBundleInvoked = false;
    public boolean wasSetupInvoked = false;
    public boolean wasTeardownInvoked = false;
    private final String name;

    public Invocations(String name) {
      this.name = name;
    }
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private DoFn.ProcessContext mockContext;
  @Mock private BoundedWindow mockWindow;
  @Mock private DoFn.InputProvider<String> mockInputProvider;
  @Mock private DoFn.OutputReceiver<String> mockOutputReceiver;

  private DoFn.ExtraContextFactory<String, String> extraContextFactory;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    this.extraContextFactory =
        new DoFn.ExtraContextFactory<String, String>() {
          @Override
          public BoundedWindow window() {
            return mockWindow;
          }

          @Override
          public DoFn.InputProvider<String> inputProvider() {
            return mockInputProvider;
          }

          @Override
          public DoFn.OutputReceiver<String> outputReceiver() {
            return mockOutputReceiver;
          }
        };
  }

  private void checkInvokeProcessElementWorks(DoFn<String, String> fn, Invocations... invocations)
      throws Exception {
    assertTrue("Need at least one invocation to check", invocations.length >= 1);
    for (Invocations invocation : invocations) {
      assertFalse(
          "Should not yet have called processElement on " + invocation.name,
          invocation.wasProcessElementInvoked);
    }
    DoFnInvokers.INSTANCE
        .newByteBuddyInvoker(fn)
        .invokeProcessElement(mockContext, extraContextFactory);
    for (Invocations invocation : invocations) {
      assertTrue(
          "Should have called processElement on " + invocation.name,
          invocation.wasProcessElementInvoked);
    }
  }

  private void checkInvokeStartBundleWorks(DoFn<String, String> fn, Invocations... invocations)
      throws Exception {
    assertTrue("Need at least one invocation to check", invocations.length >= 1);
    for (Invocations invocation : invocations) {
      assertFalse(
          "Should not yet have called startBundle on " + invocation.name,
          invocation.wasStartBundleInvoked);
    }
    DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn).invokeStartBundle(mockContext);
    for (Invocations invocation : invocations) {
      assertTrue(
          "Should have called startBundle on " + invocation.name, invocation.wasStartBundleInvoked);
    }
  }

  private void checkInvokeFinishBundleWorks(DoFn<String, String> fn, Invocations... invocations)
      throws Exception {
    assertTrue("Need at least one invocation to check", invocations.length >= 1);
    for (Invocations invocation : invocations) {
      assertFalse(
          "Should not yet have called finishBundle on " + invocation.name,
          invocation.wasFinishBundleInvoked);
    }
    DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn).invokeFinishBundle(mockContext);
    for (Invocations invocation : invocations) {
      assertTrue(
          "Should have called finishBundle on " + invocation.name,
          invocation.wasFinishBundleInvoked);
    }
  }

  private void checkInvokeSetupWorks(DoFn<String, String> fn, Invocations... invocations)
      throws Exception {
    assertTrue("Need at least one invocation to check", invocations.length >= 1);
    for (Invocations invocation : invocations) {
      assertFalse(
          "Should not yet have called setup on " + invocation.name, invocation.wasSetupInvoked);
    }
    DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn).invokeSetup();
    for (Invocations invocation : invocations) {
      assertTrue("Should have called setup on " + invocation.name, invocation.wasSetupInvoked);
    }
  }

  private void checkInvokeTeardownWorks(DoFn<String, String> fn, Invocations... invocations)
      throws Exception {
    assertTrue("Need at least one invocation to check", invocations.length >= 1);
    for (Invocations invocation : invocations) {
      assertFalse(
          "Should not yet have called teardown on " + invocation.name,
          invocation.wasTeardownInvoked);
    }
    DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn).invokeTeardown();
    for (Invocations invocation : invocations) {
      assertTrue(
          "Should have called teardown on " + invocation.name, invocation.wasTeardownInvoked);
    }
  }

  @Test
  public void testDoFnWithNoExtraContext() throws Exception {
    final Invocations invocations = new Invocations("AnonymousClass");
    DoFn<String, String> fn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            invocations.wasProcessElementInvoked = true;
            assertSame(c, mockContext);
          }
        };

    assertFalse(
        DoFnSignatures.INSTANCE
            .getOrParseSignature(fn.getClass())
            .processElement()
            .usesSingleWindow());

    checkInvokeProcessElementWorks(fn, invocations);
  }

  @Test
  public void testDoFnInvokersReused() throws Exception {
    // Ensures that we don't create a new Invoker class for every instance of the DoFn.
    IdentityParent fn1 = new IdentityParent();
    IdentityParent fn2 = new IdentityParent();
    assertSame(
        "Invoker classes should only be generated once for each type",
        DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn1).getClass(),
        DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn2).getClass());
  }

  interface InterfaceWithProcessElement {
    @DoFn.ProcessElement
    void processElement(DoFn<String, String>.ProcessContext c);
  }

  interface LayersOfInterfaces extends InterfaceWithProcessElement {}

  private class IdentityUsingInterfaceWithProcessElement extends DoFn<String, String>
      implements LayersOfInterfaces {

    private Invocations invocations = new Invocations("Named Class");

    @Override
    public void processElement(DoFn<String, String>.ProcessContext c) {
      invocations.wasProcessElementInvoked = true;
      assertSame(c, mockContext);
    }
  }

  @Test
  public void testDoFnWithProcessElementInterface() throws Exception {
    IdentityUsingInterfaceWithProcessElement fn = new IdentityUsingInterfaceWithProcessElement();
    assertFalse(
        DoFnSignatures.INSTANCE
            .getOrParseSignature(fn.getClass())
            .processElement()
            .usesSingleWindow());
    checkInvokeProcessElementWorks(fn, fn.invocations);
  }

  private class IdentityParent extends DoFn<String, String> {
    protected Invocations parentInvocations = new Invocations("IdentityParent");

    @ProcessElement
    public void process(ProcessContext c) {
      parentInvocations.wasProcessElementInvoked = true;
      assertSame(c, mockContext);
    }
  }

  private class IdentityChildWithoutOverride extends IdentityParent {}

  private class IdentityChildWithOverride extends IdentityParent {
    protected Invocations childInvocations = new Invocations("IdentityChildWithOverride");

    @Override
    public void process(DoFn<String, String>.ProcessContext c) {
      super.process(c);
      childInvocations.wasProcessElementInvoked = true;
    }
  }

  @Test
  public void testDoFnWithMethodInSuperclass() throws Exception {
    IdentityChildWithoutOverride fn = new IdentityChildWithoutOverride();
    assertFalse(
        DoFnSignatures.INSTANCE
            .getOrParseSignature(fn.getClass())
            .processElement()
            .usesSingleWindow());
    checkInvokeProcessElementWorks(fn, fn.parentInvocations);
  }

  @Test
  public void testDoFnWithMethodInSubclass() throws Exception {
    IdentityChildWithOverride fn = new IdentityChildWithOverride();
    assertFalse(
        DoFnSignatures.INSTANCE
            .getOrParseSignature(fn.getClass())
            .processElement()
            .usesSingleWindow());
    checkInvokeProcessElementWorks(fn, fn.parentInvocations, fn.childInvocations);
  }

  @Test
  public void testDoFnWithWindow() throws Exception {
    final Invocations invocations = new Invocations("AnonymousClass");
    DoFn<String, String> fn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c, BoundedWindow w) throws Exception {
            invocations.wasProcessElementInvoked = true;
            assertSame(c, mockContext);
            assertSame(w, mockWindow);
          }
        };

    assertTrue(
        DoFnSignatures.INSTANCE
            .getOrParseSignature(fn.getClass())
            .processElement()
            .usesSingleWindow());

    checkInvokeProcessElementWorks(fn, invocations);
  }

  @Test
  public void testDoFnWithOutputReceiver() throws Exception {
    final Invocations invocations = new Invocations("AnonymousClass");
    DoFn<String, String> fn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c, OutputReceiver<String> o) throws Exception {
            invocations.wasProcessElementInvoked = true;
            assertSame(c, mockContext);
            assertSame(o, mockOutputReceiver);
          }
        };

    assertFalse(
        DoFnSignatures.INSTANCE
            .getOrParseSignature(fn.getClass())
            .processElement()
            .usesSingleWindow());

    checkInvokeProcessElementWorks(fn, invocations);
  }

  @Test
  public void testDoFnWithInputProvider() throws Exception {
    final Invocations invocations = new Invocations("AnonymousClass");
    DoFn<String, String> fn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c, InputProvider<String> i) throws Exception {
            invocations.wasProcessElementInvoked = true;
            assertSame(c, mockContext);
            assertSame(i, mockInputProvider);
          }
        };

    assertFalse(
        DoFnSignatures.INSTANCE
            .getOrParseSignature(fn.getClass())
            .processElement()
            .usesSingleWindow());

    checkInvokeProcessElementWorks(fn, invocations);
  }

  @Test
  public void testDoFnWithStartBundle() throws Exception {
    final Invocations invocations = new Invocations("AnonymousClass");
    DoFn<String, String> fn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(@SuppressWarnings("unused") ProcessContext c) {}

          @StartBundle
          public void startBundle(Context c) {
            invocations.wasStartBundleInvoked = true;
            assertSame(c, mockContext);
          }

          @FinishBundle
          public void finishBundle(Context c) {
            invocations.wasFinishBundleInvoked = true;
            assertSame(c, mockContext);
          }
        };

    checkInvokeStartBundleWorks(fn, invocations);
    checkInvokeFinishBundleWorks(fn, invocations);
  }

  @Test
  public void testDoFnWithSetupTeardown() throws Exception {
    final Invocations invocations = new Invocations("AnonymousClass");
    DoFn<String, String> fn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(@SuppressWarnings("unused") ProcessContext c) {}

          @StartBundle
          public void startBundle(Context c) {
            invocations.wasStartBundleInvoked = true;
            assertSame(c, mockContext);
          }

          @FinishBundle
          public void finishBundle(Context c) {
            invocations.wasFinishBundleInvoked = true;
            assertSame(c, mockContext);
          }

          @Setup
          public void before() {
            invocations.wasSetupInvoked = true;
          }

          @Teardown
          public void after() {
            invocations.wasTeardownInvoked = true;
          }
        };

    checkInvokeSetupWorks(fn, invocations);
    checkInvokeTeardownWorks(fn, invocations);
  }

  private static class PrivateDoFnClass extends DoFn<String, String> {
    final Invocations invocations = new Invocations(getClass().getName());

    @ProcessElement
    public void processThis(ProcessContext c) {
      invocations.wasProcessElementInvoked = true;
    }
  }

  @Test
  public void testLocalPrivateDoFnClass() throws Exception {
    PrivateDoFnClass fn = new PrivateDoFnClass();
    checkInvokeProcessElementWorks(fn, fn.invocations);
  }

  @Test
  public void testStaticPackagePrivateDoFnClass() throws Exception {
    Invocations invocations = new Invocations("StaticPackagePrivateDoFn");
    checkInvokeProcessElementWorks(
        DoFnInvokersTestHelper.newStaticPackagePrivateDoFn(invocations), invocations);
  }

  @Test
  public void testInnerPackagePrivateDoFnClass() throws Exception {
    Invocations invocations = new Invocations("InnerPackagePrivateDoFn");
    checkInvokeProcessElementWorks(
        new DoFnInvokersTestHelper().newInnerPackagePrivateDoFn(invocations), invocations);
  }

  @Test
  public void testStaticPrivateDoFnClass() throws Exception {
    Invocations invocations = new Invocations("StaticPrivateDoFn");
    checkInvokeProcessElementWorks(
        DoFnInvokersTestHelper.newStaticPrivateDoFn(invocations), invocations);
  }

  @Test
  public void testInnerPrivateDoFnClass() throws Exception {
    Invocations invocations = new Invocations("StaticInnerDoFn");
    checkInvokeProcessElementWorks(
        new DoFnInvokersTestHelper().newInnerPrivateDoFn(invocations), invocations);
  }

  @Test
  public void testAnonymousInnerDoFnInOtherPackage() throws Exception {
    Invocations invocations = new Invocations("AnonymousInnerDoFnInOtherPackage");
    checkInvokeProcessElementWorks(
        new DoFnInvokersTestHelper().newInnerAnonymousDoFn(invocations), invocations);
  }

  @Test
  public void testStaticAnonymousDoFnInOtherPackage() throws Exception {
    Invocations invocations = new Invocations("AnonymousStaticDoFnInOtherPackage");
    checkInvokeProcessElementWorks(
        DoFnInvokersTestHelper.newStaticAnonymousDoFn(invocations), invocations);
  }

  @Test
  public void testProcessElementException() throws Exception {
    DoFn<Integer, Integer> fn =
        new DoFn<Integer, Integer>() {
          @ProcessElement
          public void processElement(@SuppressWarnings("unused") ProcessContext c) {
            throw new IllegalArgumentException("bogus");
          }
        };

    thrown.expect(UserCodeException.class);
    thrown.expectMessage("bogus");
    DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn).invokeProcessElement(null, null);
  }

  @Test
  public void testStartBundleException() throws Exception {
    DoFn<Integer, Integer> fn =
        new DoFn<Integer, Integer>() {
          @StartBundle
          public void startBundle(@SuppressWarnings("unused") Context c) {
            throw new IllegalArgumentException("bogus");
          }

          @ProcessElement
          public void processElement(@SuppressWarnings("unused") ProcessContext c) {}
        };

    thrown.expect(UserCodeException.class);
    thrown.expectMessage("bogus");
    DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn).invokeStartBundle(null);
  }

  @Test
  public void testFinishBundleException() throws Exception {
    DoFn<Integer, Integer> fn =
        new DoFn<Integer, Integer>() {
          @FinishBundle
          public void finishBundle(@SuppressWarnings("unused") Context c) {
            throw new IllegalArgumentException("bogus");
          }

          @ProcessElement
          public void processElement(@SuppressWarnings("unused") ProcessContext c) {}
        };

    thrown.expect(UserCodeException.class);
    thrown.expectMessage("bogus");
    DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn).invokeFinishBundle(null);
  }
}
