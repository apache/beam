/**
 * Copyright 2016 Seznam a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.storage;

import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.Objects;

public final class ReducingMerger<T> implements ReduceFunction<T> {
  private final BinaryFunction<T, T, T> merger;

  public ReducingMerger(BinaryFunction<T, T, T> merger) {
    this.merger = Objects.requireNonNull(merger);
  }

  @Override
  public T reduce(T value1, T value2) throws Exception {
    return merger.apply(value1, value2);
  }
}