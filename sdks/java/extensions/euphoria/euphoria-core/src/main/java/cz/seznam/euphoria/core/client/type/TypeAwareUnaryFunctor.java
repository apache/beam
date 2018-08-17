/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;

public class TypeAwareUnaryFunctor<I, O>
    extends AbstractTypeAware<UnaryFunctor<I, O>, O>
    implements UnaryFunctor<I, O> {

  private TypeAwareUnaryFunctor(UnaryFunctor<I, O> function, TypeHint<O> resultType) {
    super(function, resultType);
  }

  @Override
  public void apply(I elem, Collector<O> collector) {
    getDelegate().apply(elem, collector);
  }

  public static <I, O> TypeAwareUnaryFunctor<I, O> of(
      UnaryFunctor<I, O> function, TypeHint<O> typeHint) {
    return new TypeAwareUnaryFunctor<>(function, typeHint);

  }

}
