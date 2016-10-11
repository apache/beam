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

import static org.apache.beam.sdk.transforms.reflect.DoFnSignaturesTestUtils.errors;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.reflect.TypeToken;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignaturesTestUtils.FakeDoFn;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateSpecs;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DoFnSignatures}. */
@RunWith(JUnit4.class)
public class DoFnSignaturesTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBadExtraContext() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Must take a single argument of type Context");

    DoFnSignatures.analyzeBundleMethod(
        errors(),
        TypeToken.of(FakeDoFn.class),
        new DoFnSignaturesTestUtils.AnonymousMethod() {
          void method(DoFn<Integer, String>.Context c, int n) {}
        }.getMethod(),
        TypeToken.of(Integer.class),
        TypeToken.of(String.class));
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
  public void testPrivateStartBundle() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("startBundle()");
    thrown.expectMessage("Must be public");
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
    thrown.expectMessage("finishBundle()");
    thrown.expectMessage("Must be public");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.INSTANCE.getOrParseSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement() {}

          @FinishBundle
          void finishBundle() {}
        }.getClass());
  }

  @Test
  public void testStateIdWithWrongType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("StateId");
    thrown.expectMessage("StateSpec");
    DoFnSignatures.INSTANCE.getOrParseSignature(
        new DoFn<String, String>() {
          @StateId("foo")
          String bizzle = "bazzle";

          @ProcessElement
          public void foo(ProcessContext context) {}
        }.getClass());
  }

  @Test
  public void testStateIdDuplicate() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Duplicate");
    thrown.expectMessage("StateId");
    thrown.expectMessage("my-state-id");
    thrown.expectMessage("myfield1");
    thrown.expectMessage("myfield2");
    DoFnSignature sig =
        DoFnSignatures.INSTANCE.getOrParseSignature(
            new DoFn<KV<String, Integer>, Long>() {
              @StateId("my-state-id")
              private final StateSpec<Object, ValueState<Integer>> myfield1 =
                  StateSpecs.value(VarIntCoder.of());

              @StateId("my-state-id")
              StateSpec<Object, ValueState<Long>> myfield2 = StateSpecs.value(VarLongCoder.of());

              @ProcessElement
              public void foo(ProcessContext context) {}
            }.getClass());
  }

  @Test
  public void testStateIdNonFinal() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("State declarations must be final");
    thrown.expectMessage("Non-final field");
    thrown.expectMessage("myfield");
    DoFnSignature sig =
        DoFnSignatures.INSTANCE.getOrParseSignature(
            new DoFn<KV<String, Integer>, Long>() {
              @StateId("my-state-id")
              private StateSpec<Object, ValueState<Integer>> myfield =
                  StateSpecs.value(VarIntCoder.of());

              @ProcessElement
              public void foo(ProcessContext context) {}
            }.getClass());
  }

  @Test
  public void testSimpleStateIdAnonymousDoFn() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.INSTANCE.getOrParseSignature(
            new DoFn<KV<String, Integer>, Long>() {
              @StateId("foo")
              private final StateSpec<Object, ValueState<Integer>> bizzle =
                  StateSpecs.value(VarIntCoder.of());

              @ProcessElement
              public void foo(ProcessContext context) {}
            }.getClass());

    assertThat(sig.stateDeclarations().size(), equalTo(1));
    DoFnSignature.StateDeclaration decl = sig.stateDeclarations().get("foo");

    assertThat(decl.id(), equalTo("foo"));
    assertThat(decl.field().getName(), equalTo("bizzle"));
    assertThat(
        decl.stateType(),
        Matchers.<TypeDescriptor<?>>equalTo(new TypeDescriptor<ValueState<Integer>>() {}));
  }

  @Test
  public void testSimpleStateIdNamedDoFn() throws Exception {
    // Test classes at the bottom of the file
    DoFnSignature sig =
        DoFnSignatures.INSTANCE.signatureForDoFn(new DoFnForTestSimpleStateIdNamedDoFn());

    assertThat(sig.stateDeclarations().size(), equalTo(1));
    DoFnSignature.StateDeclaration decl = sig.stateDeclarations().get("foo");

    assertThat(decl.id(), equalTo("foo"));
    assertThat(
        decl.field(), equalTo(DoFnForTestSimpleStateIdNamedDoFn.class.getDeclaredField("bizzle")));
    assertThat(
        decl.stateType(),
        Matchers.<TypeDescriptor<?>>equalTo(new TypeDescriptor<ValueState<Integer>>() {}));
  }

  @Test
  public void testGenericStatefulDoFn() throws Exception {
    // Test classes at the bottom of the file
    DoFn<KV<String, Integer>, Long> myDoFn = new DoFnForTestGenericStatefulDoFn<Integer>(){};

    DoFnSignature sig = DoFnSignatures.INSTANCE.signatureForDoFn(myDoFn);

    assertThat(sig.stateDeclarations().size(), equalTo(1));
    DoFnSignature.StateDeclaration decl = sig.stateDeclarations().get("foo");

    assertThat(decl.id(), equalTo("foo"));
    assertThat(
        decl.field(), equalTo(DoFnForTestGenericStatefulDoFn.class.getDeclaredField("bizzle")));
    assertThat(
        decl.stateType(),
        Matchers.<TypeDescriptor<?>>equalTo(new TypeDescriptor<ValueState<Integer>>() {}));
  }

  private static class DoFnForTestSimpleStateIdNamedDoFn extends DoFn<KV<String, Integer>, Long> {
    @StateId("foo")
    private final StateSpec<Object, ValueState<Integer>> bizzle =
        StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void foo(ProcessContext context) {}
  }

  private static class DoFnForTestGenericStatefulDoFn<T> extends DoFn<KV<String, T>, Long> {
    // Note that in order to have a coder for T it will require initialization in the constructor,
    // but that isn't important for this test
    @StateId("foo")
    private final StateSpec<Object, ValueState<T>> bizzle = null;

    @ProcessElement
    public void foo(ProcessContext context) {}
  }
}
