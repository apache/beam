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

import java.lang.reflect.Method;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures.FnAnalysisContext;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Utilities for use in {@link DoFnSignatures} tests. */
class DoFnSignaturesTestUtils {
  /** An empty base {@link DoFn} class. */
  static class FakeDoFn extends DoFn<Integer, String> {}

  /** An error reporter. */
  static DoFnSignatures.ErrorReporter errors() {
    return new DoFnSignatures.ErrorReporter(null, "[test]");
  }

  /**
   * A class for testing utilities that take {@link Method} objects. Use like this:
   *
   * <pre>{@code
   * Method m = new AnonymousMethod() {
   *   SomeReturnValue method(SomeParameters...) { ... }
   * }.getMethod();  // Will return the Method for "method".
   * }</pre>
   */
  static class AnonymousMethod {
    final Method getMethod() throws Exception {
      for (Method method : getClass().getDeclaredMethods()) {
        if ("method".equals(method.getName())) {
          return method;
        }
      }
      throw new NoSuchElementException("No method named 'method' defined on " + getClass());
    }
  }

  static DoFnSignature.ProcessElementMethod analyzeProcessElementMethod(AnonymousMethod method)
      throws Exception {
    return DoFnSignatures.analyzeProcessElementMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        method.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        FnAnalysisContext.create());
  }
}
