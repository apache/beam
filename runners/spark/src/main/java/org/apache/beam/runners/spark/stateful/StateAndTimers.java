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
package org.apache.beam.runners.spark.stateful;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table;

/** State and Timers wrapper. */
@AutoValue
public abstract class StateAndTimers implements Serializable {
  public abstract Table<String, String, byte[]> getState();

  public abstract Collection<byte[]> getTimers();

  public static StateAndTimers of(
      final Table<String, String, byte[]> state, final Collection<byte[]> timers) {
    return new AutoValue_StateAndTimers.Builder().setState(state).setTimers(timers).build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setState(Table<String, String, byte[]> state);

    abstract Builder setTimers(Collection<byte[]> timers);

    abstract StateAndTimers build();
  }
}
