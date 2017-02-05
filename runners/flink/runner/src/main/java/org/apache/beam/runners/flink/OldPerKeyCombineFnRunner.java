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
package org.apache.beam.runners.flink;

import java.io.Serializable;
import org.apache.beam.runners.core.OldDoFn;
import org.apache.beam.sdk.transforms.CombineFnBase.PerKeyCombineFn;

/**
 * An interface that runs a {@link PerKeyCombineFn} with unified APIs using
 * {@link OldDoFn.ProcessContext}.
 */
@Deprecated
public interface OldPerKeyCombineFnRunner<K, InputT, AccumT, OutputT> extends Serializable {
  /**
   * Forwards the call to a {@link PerKeyCombineFn} to create the accumulator in a {@link OldDoFn}.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from {@code OldDoFn.ProcessContext}
   * if it is required.
   */
  AccumT createAccumulator(K key, OldDoFn<?, ?>.ProcessContext c);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to add the input in a {@link OldDoFn}.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from {@code OldDoFn.ProcessContext}
   * if it is required.
   */
  AccumT addInput(K key, AccumT accumulator, InputT input, OldDoFn<?, ?>.ProcessContext c);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to merge accumulators in a {@link OldDoFn}.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from {@code OldDoFn.ProcessContext}
   * if it is required.
   */
  AccumT mergeAccumulators(
      K key, Iterable<AccumT> accumulators, OldDoFn<?, ?>.ProcessContext c);

  /**
   * Forwards the call to a {@link PerKeyCombineFn} to extract the output in a {@link OldDoFn}.
   *
   * <p>It constructs a {@code CombineWithContext.Context} from {@code OldDoFn.ProcessContext}
   * if it is required.
   */
  OutputT extractOutput(K key, AccumT accumulator, OldDoFn<?, ?>.ProcessContext c);
}
