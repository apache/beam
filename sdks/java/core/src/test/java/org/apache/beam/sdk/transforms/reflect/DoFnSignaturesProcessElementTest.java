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

import static org.apache.beam.sdk.transforms.reflect.DoFnSignaturesTestUtils.analyzeProcessElementMethod;

import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignaturesTestUtils.AnonymousMethod;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DoFnSignatures} verification of the {@link DoFn.ProcessElement} method. */
@SuppressWarnings("unused")
@RunWith(JUnit4.class)
public class DoFnSignaturesProcessElementTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBadExtraProcessContextType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Integer is not a valid context parameter.");

    analyzeProcessElementMethod(
        new AnonymousMethod() {
          private void method(DoFn<Integer, String>.ProcessContext c, Integer n) {}
        });
  }

  @Test
  public void testBadReturnType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Must return void or ProcessContinuation");

    analyzeProcessElementMethod(
        new AnonymousMethod() {
          private int method(DoFn<Integer, String>.ProcessContext context) {
            return 0;
          }
        });
  }

  @Test
  public void testGoodConcreteTypes() throws Exception {
    analyzeProcessElementMethod(
        new AnonymousMethod() {
          private void method(DoFn<Integer, String>.ProcessContext c) {}
        });
  }

  @Test
  public void testBadGenericsTwoArgs() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("DoFn<Integer, Integer>.ProcessContext");
    thrown.expectMessage("must have type");
    thrown.expectMessage("DoFn<Integer, String>.ProcessContext");

    analyzeProcessElementMethod(
        new AnonymousMethod() {
          private void method(DoFn<Integer, Integer>.ProcessContext c) {}
        });
  }

  @Test
  public void testBadGenericWildCards() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("DoFn<Integer, ? super Integer>.ProcessContext");
    thrown.expectMessage("must have type");
    thrown.expectMessage("DoFn<Integer, String>.ProcessContext");

    analyzeProcessElementMethod(
        new AnonymousMethod() {
          private void method(DoFn<Integer, ? super Integer>.ProcessContext c) {}
        });
  }

  static class BadTypeVariables<InputT, OutputT> extends DoFn<InputT, OutputT> {
    @ProcessElement
    @SuppressWarnings("unused")
    public void badTypeVariables(DoFn<InputT, InputT>.ProcessContext c) {}
  }

  @Test
  public void testBadTypeVariables() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("DoFn<InputT, InputT>.ProcessContext");
    thrown.expectMessage("must have type");
    thrown.expectMessage("DoFn<InputT, OutputT>.ProcessContext");

    DoFnSignatures.getSignature(BadTypeVariables.class);
  }

  @Test
  public void testNoProcessElement() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No method annotated with @ProcessElement found");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.getSignature(new DoFn<String, String>() {}.getClass());
  }

  @Test
  public void testMultipleProcessElement() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Found multiple methods annotated with @ProcessElement");
    thrown.expectMessage("foo()");
    thrown.expectMessage("bar()");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.getSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void foo() {}

          @ProcessElement
          public void bar() {}
        }.getClass());
  }

  @Test
  public void testPrivateProcessElement() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("process()");
    thrown.expectMessage("Must be public");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.getSignature(
        new DoFn<String, String>() {
          @ProcessElement
          private void process() {}
        }.getClass());
  }

  private static class GoodTypeVariables<InputT, OutputT> extends DoFn<InputT, OutputT> {
    @ProcessElement
    @SuppressWarnings("unused")
    public void goodTypeVariables(DoFn<InputT, OutputT>.ProcessContext c) {}
  }

  @Test
  public void testGoodTypeVariables() throws Exception {
    DoFnSignatures.getSignature(GoodTypeVariables.class);
  }

  private static class IdentityFn<T> extends DoFn<T, T> {
    @ProcessElement
    @SuppressWarnings("unused")
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  private static class IdentityListFn<T> extends IdentityFn<List<T>> {}

  @Test
  public void testIdentityFnApplied() throws Exception {
    DoFnSignatures.getSignature(new IdentityFn<String>() {}.getClass());
  }
}
