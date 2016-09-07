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

import com.google.common.reflect.TypeToken;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignaturesTestUtils.FakeDoFn;
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
}
