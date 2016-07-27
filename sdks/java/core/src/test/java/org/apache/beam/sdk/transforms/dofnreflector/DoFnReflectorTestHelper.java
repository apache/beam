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
package org.apache.beam.sdk.transforms.dofnreflector;

import org.apache.beam.sdk.transforms.DoFnWithContext;
import org.apache.beam.sdk.transforms.DoFnWithContext.ProcessElement;

/**
 * Test helper for DoFnReflectorTest, which needs to test package-private access
 * to DoFns in other packages.
 */
public class DoFnReflectorTestHelper {

  private static class StaticPrivateDoFn extends DoFnWithContext<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {}
  }

  private class NestedPrivateDoFn extends DoFnWithContext<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {}
  }

  static class StaticPackagePrivateDoFn extends DoFnWithContext<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {}
  }

  class NestedPackagePrivateDoFn extends DoFnWithContext<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {}
  }

  public static DoFnWithContext<String, String> newStaticPackagePrivateDoFn() {
    return new StaticPackagePrivateDoFn();
  }

  public DoFnWithContext<String, String> newNestedPackagePrivateDoFn() {
    return new NestedPackagePrivateDoFn();
  }

  public static DoFnWithContext<String, String> newStaticPrivateDoFn() {
    return new StaticPrivateDoFn();
  }

  public DoFnWithContext<String, String> newNestedPrivateDoFn() {
    return new NestedPrivateDoFn();
  }

  public DoFnWithContext<String, String> newNestedAnonymousDoFn() {
    return new DoFnWithContext<String, String>() {
      @ProcessElement
      public void process(ProcessContext c) {}
    };
  }

  public static DoFnWithContext<String, String> newStaticAnonymousDoFn() {
    return new DoFnWithContext<String, String>() {
      @ProcessElement
      public void process(ProcessContext c) {}
    };
  }
}
