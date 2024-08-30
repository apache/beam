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
package org.apache.beam.runners.dataflow.worker.streaming.sideinput;

import com.google.auto.value.AutoValue;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Entry in the side input cache that stores the value and the encoded size of the value.
 *
 * <p>Can be in 1 of 3 states:
 *
 * <ul>
 *   <li>Ready with a <T> value.
 *   <li>Ready with no value, represented as {@link Optional<T>}
 *   <li>Not ready.
 * </ul>
 */
@AutoValue
public abstract class SideInput<T> {
  static <T> SideInput<T> ready(@Nullable T value, int encodedSize) {
    return new AutoValue_SideInput<>(true, Optional.ofNullable(value), encodedSize);
  }

  static <T> SideInput<T> notReady() {
    return new AutoValue_SideInput<>(false, Optional.empty(), 0);
  }

  public abstract boolean isReady();

  public abstract Optional<T> value();

  public abstract int size();
}
