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
package org.apache.beam.runners.core.triggers;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

/** No-op {@link TriggerStateMachine} implementation for testing. */
abstract class StubTriggerStateMachine extends TriggerStateMachine {
  /**
   * Create a stub {@link TriggerStateMachine} instance which returns the specified name on {@link
   * #toString()}.
   */
  static StubTriggerStateMachine named(final String name) {
    return new StubTriggerStateMachine() {
      @Override
      public String toString() {
        return name;
      }
    };
  }

  protected StubTriggerStateMachine() {
    super(Lists.newArrayList());
  }

  @Override
  public void onFire(TriggerContext context) throws Exception {}

  @Override
  public void onElement(OnElementContext c) throws Exception {}

  @Override
  public void onMerge(OnMergeContext c) throws Exception {}

  @Override
  public boolean shouldFire(TriggerContext context) throws Exception {
    return false;
  }
}
