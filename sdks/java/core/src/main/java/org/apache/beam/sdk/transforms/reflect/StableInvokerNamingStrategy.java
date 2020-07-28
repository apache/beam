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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.NamingStrategy;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.type.TypeDescription;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A naming strategy for ByteBuddy invokers ({@link DoFnInvoker} and {@link OnTimerInvoker}) that is
 * deterministic and readable. This is correct to use only when a class is created at most once.
 */
@AutoValue
abstract class StableInvokerNamingStrategy extends NamingStrategy.AbstractBase {

  public abstract Class<? extends DoFn<?, ?>> getFnClass();

  public abstract @Nullable String getSuffix();

  public static StableInvokerNamingStrategy forDoFnClass(Class<? extends DoFn<?, ?>> fnClass) {
    return new AutoValue_StableInvokerNamingStrategy(fnClass, null);
  }

  public StableInvokerNamingStrategy withSuffix(String newSuffix) {
    return new AutoValue_StableInvokerNamingStrategy(getFnClass(), newSuffix);
  }

  @Override
  protected String name(TypeDescription superClass) {
    return String.format(
        "%s$%s",
        getFnClass().getName(), firstNonNull(getSuffix(), superClass.getName().replace(".", "_")));
  }
}
