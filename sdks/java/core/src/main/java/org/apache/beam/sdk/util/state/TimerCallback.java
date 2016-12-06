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
package org.apache.beam.sdk.util.state;

import org.apache.beam.sdk.util.TimerInternals;

/**
 * A callback that processes a {@link TimerInternals.TimerData TimerData}.
 *
 * @deprecated Use InMemoryTimerInternals advance and remove methods instead of callback.
 */
@Deprecated
public interface TimerCallback {
  /** Processes the {@link TimerInternals.TimerData TimerData}. */
  void onTimer(TimerInternals.TimerData timer) throws Exception;

  TimerCallback NO_OP = new TimerCallback() {
    @Override
    public void onTimer(TimerInternals.TimerData timer) throws Exception {
      // Nothing
    }
  };
}
