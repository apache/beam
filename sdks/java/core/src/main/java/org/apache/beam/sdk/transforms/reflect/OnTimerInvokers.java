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
import org.apache.beam.sdk.transforms.DoFn.OnTimer;
import org.apache.beam.sdk.transforms.DoFn.TimerId;

/**
 * Dynamically generates {@link OnTimerInvoker} instances for invoking a particular {@link TimerId}
 * on a particular {@link DoFn}.
 */
class OnTimerInvokers {

  /**
   * Returns an invoker that will call the given {@link DoFn DoFn's} {@link OnTimer @OnTimer} method
   * for the given {@code timerId}, using a default choice of {@link OnTimerInvokerFactory}.
   *
   * <p>The default is permitted to change at any time. Users of this method may not depend on any
   * details {@link OnTimerInvokerFactory}-specific details of the invoker. Today it is {@link
   * ByteBuddyOnTimerInvokerFactory}.
   */
  public static <InputT, OutputT> OnTimerInvoker<InputT, OutputT> forTimer(
      DoFn<InputT, OutputT> fn, String timerId) {
    return ByteBuddyOnTimerInvokerFactory.only().forTimer(fn, timerId);
  }

  public static <InputT, OutputT> OnTimerInvoker<InputT, OutputT> forTimerFamily(
      DoFn<InputT, OutputT> fn, String timerFamilyId) {
    return ByteBuddyOnTimerInvokerFactory.only().forTimerFamily(fn, timerFamilyId);
  }
}
