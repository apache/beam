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

import org.apache.beam.sdk.transforms.DoFn;

import com.google.common.reflect.TypeToken;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.lang.reflect.Method;
import java.util.List;

/** Tests for {@link DoFnSignatures}. */
@RunWith(JUnit4.class)
public class DoFnSignaturesTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static class FakeDoFn extends DoFn<Integer, String> {}

  @SuppressWarnings({"unused"})
  private void missingProcessContext() {}

  @Test
  public void testMissingProcessContext() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        getClass().getName()
            + "#missingProcessContext() must take a ProcessContext<> as its first argument");

    DoFnSignatures.analyzeProcessElementMethod(
        TypeToken.of(FakeDoFn.class),
        getClass().getDeclaredMethod("missingProcessContext"),
        TypeToken.of(Integer.class),
        TypeToken.of(String.class));
  }

  @SuppressWarnings({"unused"})
  private void badProcessContext(String s) {}

  @Test
  public void testBadProcessContextType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        getClass().getName()
            + "#badProcessContext(String) must take a ProcessContext<> as its first argument");

    DoFnSignatures.analyzeProcessElementMethod(
        TypeToken.of(FakeDoFn.class),
        getClass().getDeclaredMethod("badProcessContext", String.class),
        TypeToken.of(Integer.class),
        TypeToken.of(String.class));
  }

  @SuppressWarnings({"unused"})
  private void badExtraContext(DoFn<Integer, String>.Context c, int n) {}

  @Test
  public void testBadExtraContext() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        getClass().getName()
            + "#badExtraContext(Context, int) must have a single argument of type Context");

    DoFnSignatures.analyzeBundleMethod(
        TypeToken.of(FakeDoFn.class),
        getClass().getDeclaredMethod("badExtraContext", DoFn.Context.class, int.class),
        TypeToken.of(Integer.class),
        TypeToken.of(String.class));
  }

  @SuppressWarnings({"unused"})
  private void badExtraProcessContext(DoFn<Integer, String>.ProcessContext c, Integer n) {}

  @Test
  public void testBadExtraProcessContextType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Integer is not a valid context parameter for method "
            + getClass().getName()
            + "#badExtraProcessContext(ProcessContext, Integer)"
            + ". Should be one of [BoundedWindow]");

    DoFnSignatures.analyzeProcessElementMethod(
        TypeToken.of(FakeDoFn.class),
        getClass()
            .getDeclaredMethod("badExtraProcessContext", DoFn.ProcessContext.class, Integer.class),
        TypeToken.of(Integer.class),
        TypeToken.of(String.class));
  }

  @SuppressWarnings("unused")
  private int badReturnType() {
    return 0;
  }

  @Test
  public void testBadReturnType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(getClass().getName() + "#badReturnType() must have a void return type");

    DoFnSignatures.analyzeProcessElementMethod(
        TypeToken.of(FakeDoFn.class),
        getClass().getDeclaredMethod("badReturnType"),
        TypeToken.of(Integer.class),
        TypeToken.of(String.class));
  }

  @SuppressWarnings("unused")
  private void goodConcreteTypes(
      DoFn<Integer, String>.ProcessContext c,
      DoFn.InputProvider<Integer> input,
      DoFn.OutputReceiver<String> output) {}

  @Test
  public void testGoodConcreteTypes() throws Exception {
    Method method =
        getClass()
            .getDeclaredMethod(
                "goodConcreteTypes",
                DoFn.ProcessContext.class,
                DoFn.InputProvider.class,
                DoFn.OutputReceiver.class);
    DoFnSignatures.analyzeProcessElementMethod(
        TypeToken.of(FakeDoFn.class),
        method,
        TypeToken.of(Integer.class),
        TypeToken.of(String.class));
  }

  private static class GoodTypeVariables<InputT, OutputT> extends DoFn<InputT, OutputT> {
    @ProcessElement
    @SuppressWarnings("unused")
    public void goodTypeVariables(
        DoFn<InputT, OutputT>.ProcessContext c,
        DoFn.InputProvider<InputT> input,
        DoFn.OutputReceiver<OutputT> output) {}
  }

  @Test
  public void testGoodTypeVariables() throws Exception {
    DoFnSignatures.INSTANCE.getOrParseSignature(GoodTypeVariables.class);
  }

  private static class IdentityFn<T> extends DoFn<T, T> {
    @ProcessElement
    @SuppressWarnings("unused")
    public void processElement(ProcessContext c, InputProvider<T> input, OutputReceiver<T> output) {
      c.output(c.element());
    }
  }

  private static class IdentityListFn<T> extends IdentityFn<List<T>> {}

  @Test
  public void testIdentityFnApplied() throws Exception {
    DoFnSignatures.INSTANCE.getOrParseSignature(new IdentityFn<String>() {}.getClass());
  }

  @SuppressWarnings("unused")
  private void badGenericTwoArgs(
      DoFn<Integer, String>.ProcessContext c,
      DoFn.InputProvider<Integer> input,
      DoFn.OutputReceiver<Integer> output) {}

  @Test
  public void testBadGenericsTwoArgs() throws Exception {
    Method method =
        getClass()
            .getDeclaredMethod(
                "badGenericTwoArgs",
                DoFn.ProcessContext.class,
                DoFn.InputProvider.class,
                DoFn.OutputReceiver.class);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Wrong type of OutputReceiver parameter "
            + "for method "
            + getClass().getName()
            + "#badGenericTwoArgs(ProcessContext, InputProvider, OutputReceiver): "
            + "OutputReceiver<Integer>, should be "
            + "OutputReceiver<String>");

    DoFnSignatures.analyzeProcessElementMethod(
        TypeToken.of(FakeDoFn.class),
        method,
        TypeToken.of(Integer.class),
        TypeToken.of(String.class));
  }

  @SuppressWarnings("unused")
  private void badGenericWildCards(
      DoFn<Integer, String>.ProcessContext c,
      DoFn.InputProvider<Integer> input,
      DoFn.OutputReceiver<? super Integer> output) {}

  @Test
  public void testBadGenericWildCards() throws Exception {
    Method method =
        getClass()
            .getDeclaredMethod(
                "badGenericWildCards",
                DoFn.ProcessContext.class,
                DoFn.InputProvider.class,
                DoFn.OutputReceiver.class);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Wrong type of OutputReceiver parameter for method "
            + getClass().getName()
            + "#badGenericWildCards(ProcessContext, InputProvider, OutputReceiver): "
            + "OutputReceiver<? super Integer>, should be "
            + "OutputReceiver<String>");

    DoFnSignatures.analyzeProcessElementMethod(
        TypeToken.of(FakeDoFn.class),
        method,
        TypeToken.of(Integer.class),
        TypeToken.of(String.class));
  }

  static class BadTypeVariables<InputT, OutputT> extends DoFn<InputT, OutputT> {
    @ProcessElement
    @SuppressWarnings("unused")
    public void badTypeVariables(
        DoFn<InputT, OutputT>.ProcessContext c,
        DoFn.InputProvider<InputT> input,
        DoFn.OutputReceiver<InputT> output) {}
  }

  @Test
  public void testBadTypeVariables() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Wrong type of OutputReceiver parameter for method "
            + BadTypeVariables.class.getName()
            + "#badTypeVariables(ProcessContext, InputProvider, OutputReceiver): "
            + "OutputReceiver<InputT>, should be "
            + "OutputReceiver<OutputT>");

    DoFnSignatures.INSTANCE.getOrParseSignature(BadTypeVariables.class);
  }

  @Test
  public void testNoProcessElement() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No method annotated with @ProcessElement found");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.INSTANCE.getOrParseSignature(new DoFn<String, String>() {}.getClass());
  }

  @Test
  public void testMultipleProcessElement() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Found multiple methods annotated with @ProcessElement");
    thrown.expectMessage("foo()");
    thrown.expectMessage("bar()");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.INSTANCE.getOrParseSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void foo() {}

          @ProcessElement
          public void bar() {}
        }.getClass());
  }

  @Test
  public void testMultipleStartBundleElement() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Found multiple methods annotated with @StartBundle");
    thrown.expectMessage("bar()");
    thrown.expectMessage("baz()");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.INSTANCE.getOrParseSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void foo() {}

          @StartBundle
          public void bar() {}

          @StartBundle
          public void baz() {}
        }.getClass());
  }

  @Test
  public void testMultipleFinishBundleMethods() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Found multiple methods annotated with @FinishBundle");
    thrown.expectMessage("bar(Context)");
    thrown.expectMessage("baz(Context)");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.INSTANCE.getOrParseSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void foo(ProcessContext context) {}

          @FinishBundle
          public void bar(Context context) {}

          @FinishBundle
          public void baz(Context context) {}
        }.getClass());
  }

  @Test
  public void testPrivateProcessElement() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("process() must be public");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.INSTANCE.getOrParseSignature(
        new DoFn<String, String>() {
          @ProcessElement
          private void process() {}
        }.getClass());
  }

  @Test
  public void testPrivateStartBundle() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("startBundle() must be public");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.INSTANCE.getOrParseSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement() {}

          @StartBundle
          void startBundle() {}
        }.getClass());
  }

  @Test
  public void testPrivateFinishBundle() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("finishBundle() must be public");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.INSTANCE.getOrParseSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement() {}

          @FinishBundle
          void finishBundle() {}
        }.getClass());
  }
}
