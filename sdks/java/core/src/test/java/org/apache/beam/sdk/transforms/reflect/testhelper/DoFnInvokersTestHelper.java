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
package org.apache.beam.sdk.transforms.reflect.testhelper;

import static org.mockito.Mockito.verify;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokersTest;
import org.mockito.Mockito;

/**
 * Test helper for {@link DoFnInvokersTest}, which needs to test package-private access to DoFns in
 * other packages.
 */
public class DoFnInvokersTestHelper {

  private static class StaticPrivateDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {}
  }

  private class InnerPrivateDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {}
  }

  static class StaticPackagePrivateDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {}
  }

  class InnerPackagePrivateDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {}
  }

  public static DoFn<String, String> newStaticPackagePrivateDoFn() {
    return new StaticPackagePrivateDoFn();
  }

  public static void verifyStaticPackagePrivateDoFn(DoFn<String, String> fn) {
    verify((StaticPackagePrivateDoFn) fn).process(Mockito.notNull(DoFn.ProcessContext.class));
  }

  public DoFn<String, String> newInnerPackagePrivateDoFn() {
    return new InnerPackagePrivateDoFn();
  }

  public static void verifyInnerPackagePrivateDoFn(DoFn<String, String> fn) {
    verify((InnerPackagePrivateDoFn) fn).process(Mockito.notNull(DoFn.ProcessContext.class));
  }

  public static DoFn<String, String> newStaticPrivateDoFn() {
    return new StaticPrivateDoFn();
  }

  public static void verifyStaticPrivateDoFn(DoFn<String, String> fn) {
    verify((StaticPrivateDoFn) fn).process(Mockito.notNull(DoFn.ProcessContext.class));
  }

  public DoFn<String, String> newInnerPrivateDoFn() {
    return new InnerPrivateDoFn();
  }

  public static void verifyInnerPrivateDoFn(DoFn<String, String> fn) {
    verify((InnerPrivateDoFn) fn).process(Mockito.notNull(DoFn.ProcessContext.class));
  }

  public DoFn<String, String> newInnerAnonymousDoFn() {
    return new DoFn<String, String>() {
      @ProcessElement
      public void process(ProcessContext c) {}
    };
  }

  public static void verifyInnerAnonymousDoFn(DoFn<String, String> fn) throws Exception {
    DoFn<String, String> verifier = verify(fn);
    verifier
        .getClass()
        .getMethod("process", DoFn.ProcessContext.class)
        .invoke(verifier, Mockito.notNull(DoFn.ProcessContext.class));
  }

  public static DoFn<String, String> newStaticAnonymousDoFn() {
    return new DoFn<String, String>() {
      private boolean invoked = false;

      @ProcessElement
      public void process(ProcessContext c) {
        invoked = true;
      }

      public boolean invoked() {
        return invoked;
      }
    };
  }

  public static boolean wasStaticAnonymousDoFnInvoked(DoFn<String, String> fn) throws Exception {
    return (Boolean) fn.getClass().getMethod("invoked").invoke(fn);
  }
}
