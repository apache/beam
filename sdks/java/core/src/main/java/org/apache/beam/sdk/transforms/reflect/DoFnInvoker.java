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

/**
 * Interface for invoking the {@code DoFn} processing methods.
 *
 * <p>Instantiating a {@link DoFnInvoker} associates it with a specific {@link DoFn} instance,
 * referred to as the bound {@link DoFn}.
 */
public interface DoFnInvoker<InputT, OutputT> {
  /** Invoke the {@link DoFn.Setup} method on the bound {@link DoFn}. */
  void invokeSetup();

  /** Invoke the {@link DoFn.StartBundle} method on the bound {@link DoFn}. */
  void invokeStartBundle(DoFn<InputT, OutputT>.Context c);

  /** Invoke the {@link DoFn.FinishBundle} method on the bound {@link DoFn}. */
  void invokeFinishBundle(DoFn<InputT, OutputT>.Context c);

  /** Invoke the {@link DoFn.Teardown} method on the bound {@link DoFn}. */
  void invokeTeardown();

  /**
   * Invoke the {@link DoFn.ProcessElement} method on the bound {@link DoFn}.
   *
   * @param c The {@link DoFn.ProcessContext} to invoke the fn with.
   * @param extra Factory for producing extra parameter objects (such as window), if necessary.
   */
  void invokeProcessElement(
      DoFn<InputT, OutputT>.ProcessContext c, DoFn.ExtraContextFactory<InputT, OutputT> extra);
}
