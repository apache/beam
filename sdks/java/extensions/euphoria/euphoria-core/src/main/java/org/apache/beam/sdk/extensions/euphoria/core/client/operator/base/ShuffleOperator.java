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

import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAware;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Operator working on some context. */
@Audience(Audience.Type.INTERNAL)
public abstract class ShuffleOperator<InputT, KeyT, OutputT> extends Operator<OutputT>
    implements TypeAware.Key<KeyT>, TypeAware.Output<OutputT>, WindowAware<InputT> {

  private final UnaryFunction<InputT, KeyT> keyExtractor;
  @Nullable private final TypeDescriptor<KeyT> keyType;
  @Nullable private final Window<InputT> window;

  protected ShuffleOperator(
      @Nullable String name,
      @Nullable TypeDescriptor<OutputT> outputType,
      UnaryFunction<InputT, KeyT> keyExtractor,
      @Nullable TypeDescriptor<KeyT> keyType,
      @Nullable Window<InputT> windowing) {
    super(name, outputType);
    this.keyExtractor = keyExtractor;
    this.keyType = keyType;
    this.window = windowing;
  }

  public UnaryFunction<InputT, KeyT> getKeyExtractor() {
    return keyExtractor;
  }

  @Override
  public Optional<TypeDescriptor<KeyT>> getKeyType() {
    return Optional.ofNullable(keyType);
  }

  @Override
  public Optional<Window<InputT>> getWindow() {
    return Optional.ofNullable(window);
  }
}
