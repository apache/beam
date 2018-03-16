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

package org.apache.beam.runners.fnexecution.data;

import com.google.auto.value.AutoValue;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;

/**
 * A pair of {@link Coder} and {@link BeamFnApi.Target} which specifies the arguments to a {@link
 * FnDataService} to send data to a remote harness.
 */
@AutoValue
public abstract class RemoteInputDestination<T> {
  public static <T> RemoteInputDestination<T> of(Coder<T> coder, BeamFnApi.Target target) {
    return new AutoValue_RemoteInputDestination<>(coder, target);
  }

  public abstract Coder<T> getCoder();
  public abstract BeamFnApi.Target getTarget();
}
