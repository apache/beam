/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.type;

import cz.seznam.euphoria.core.client.functional.TypeHintAware;
import java.io.Serializable;
import java.util.Objects;

abstract class AbstractTypeAware<FuncT, T> implements Serializable, TypeHintAware<T> {

  private final FuncT function;
  private final TypeHint<T> typeHint;

  AbstractTypeAware(FuncT function, TypeHint<T> typeHint) {
    this.function = Objects.requireNonNull(function);
    this.typeHint = Objects.requireNonNull(typeHint);
  }

  public FuncT getDelegate() {
    return function;
  }

  @Override
  public TypeHint<T> getTypeHint() {
    return typeHint;
  }
}
