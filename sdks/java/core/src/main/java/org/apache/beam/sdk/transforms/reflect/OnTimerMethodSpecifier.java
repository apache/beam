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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Used by {@link ByteBuddyOnTimerInvokerFactory} to Dynamically generate
 * {@link OnTimerInvoker} instances for invoking a particular
 * {@link DoFn.TimerId} on a particular {@link DoFn}.
 */

@AutoValue
abstract class OnTimerMethodSpecifier {
    public abstract Class<? extends DoFn<?, ?>> fnClass();
    public abstract String timerId();
    public static OnTimerMethodSpecifier
    forClassAndTimerId(Class<? extends DoFn<?, ?>> fnClass, String timerId) {
        return  new AutoValue_OnTimerMethodSpecifier(fnClass, timerId);
    }
}
