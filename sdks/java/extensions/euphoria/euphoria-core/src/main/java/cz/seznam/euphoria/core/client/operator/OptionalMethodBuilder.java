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

package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import java.util.Objects;

/**
 * Class to be extended by operator builders that want to make use
 * of `applyIf` call.
 *
 * @param <BUILDER> the class of the builder that extends this class
 */
public interface OptionalMethodBuilder<BUILDER> {

  @SuppressWarnings("unchecked")
  default BUILDER applyIf(boolean cond, UnaryFunction<BUILDER, BUILDER> apply) {
    Objects.requireNonNull(apply);
    return cond ? apply.apply((BUILDER) this) : (BUILDER) this;
  }

}
