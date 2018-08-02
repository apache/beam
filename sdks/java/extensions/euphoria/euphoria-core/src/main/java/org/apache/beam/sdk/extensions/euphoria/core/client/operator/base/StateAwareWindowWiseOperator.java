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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator.base;

import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAware;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Operator with internal state. */
@Audience(Audience.Type.INTERNAL)
public abstract class StateAwareWindowWiseOperator<
        InputT,
        KeyInT,
        K,
        OutputT,
        W extends BoundedWindow,
        OperatorT extends StateAwareWindowWiseOperator<InputT, KeyInT, K, OutputT, W, OperatorT>>
    extends WindowWiseOperator<InputT, OutputT, W>
    implements StateAware<KeyInT, K>, TypeAware.Key<K> {

  protected final UnaryFunction<KeyInT, K> keyExtractor;
  @Nullable protected final TypeDescriptor<K> keyType;

  protected StateAwareWindowWiseOperator(
      String name,
      Flow flow,
      @Nullable TypeDescriptor<OutputT> outputType,
      @Nullable WindowingDesc<Object, W> windowing,
      @Nullable Windowing euphoriaWindowing,
      UnaryFunction<KeyInT, K> keyExtractor,
      TypeDescriptor<K> keyType) {

    super(name, flow, outputType, windowing, euphoriaWindowing);
    this.keyExtractor = keyExtractor;
    this.keyType = keyType;
  }

  @Override
  public UnaryFunction<KeyInT, K> getKeyExtractor() {
    return keyExtractor;
  }

  @Override
  public TypeDescriptor<K> getKeyType() {
    return keyType;
  }
}
