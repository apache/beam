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

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.DoFn;

/** Static utilities for working with {@link DoFnInvoker}. */
@Internal
public class DoFnInvokers {

  /**
   * Returns an {@link DoFnInvoker} for the given {@link DoFn}, using a default choice of {@link
   * DoFnInvokerFactory}.
   *
   * <p>The default is permitted to change at any time. Users of this method may not depend on any
   * details {@link DoFnInvokerFactory}-specific details of the invoker. Today it is {@link
   * ByteBuddyDoFnInvokerFactory}.
   */
  public static <InputT, OutputT> DoFnInvoker<InputT, OutputT> invokerFor(
      DoFn<InputT, OutputT> fn) {
    return ByteBuddyDoFnInvokerFactory.only().newByteBuddyInvoker(fn);
  }

  private DoFnInvokers() {}

  /**
   * Tries to invoke setup on the given {@link DoFn}. If setup throws any exception, the given
   * {@link DoFn} will be torn down immediately and the exception rethrown.
   *
   * <p>On success returns an {@link DoFnInvoker} for the given {@link DoFn}.
   */
  public static <InputT, OutputT> DoFnInvoker<InputT, OutputT> tryInvokeSetupFor(
      DoFn<InputT, OutputT> fn) {
    DoFnInvoker<InputT, OutputT> doFnInvoker = invokerFor(fn);
    try {
      doFnInvoker.invokeSetup();
    } catch (Exception e) {
      try {
        doFnInvoker.invokeTeardown();
      } catch (Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
    return doFnInvoker;
  }
}
