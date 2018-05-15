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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import java.util.Objects;

/**
 * Class to be extended by operator builders that want to make use of `applyIf` call.
 *
 * @param <BuilderT> the class of the builder that extends this class
 */
@Audience(Audience.Type.INTERNAL)
public interface OptionalMethodBuilder<BuilderT> {

  /**
   * Apply given modification to builder when condition evaluates to {@code true}.
   *
   * @param cond the condition
   * @param apply the modification
   * @return next step builder
   */
  @SuppressWarnings("unchecked")
  default BuilderT applyIf(boolean cond, UnaryFunction<BuilderT, BuilderT> apply) {
    Objects.requireNonNull(apply);
    return cond ? apply.apply((BuilderT) this) : (BuilderT) this;
  }

  /**
   * Apply given modifications to builder based on condition.
   *
   * @param cond the condition to evaluate
   * @param applyTrue modification to ap ply when {@code cond} evaluates to {@code true}
   * @param applyFalse modification to apply when {@code cond} evaluates to {@code false}
   * @return next step builder
   */
  @SuppressWarnings("unchecked")
  default BuilderT applyIf(
      boolean cond,
      UnaryFunction<BuilderT, BuilderT> applyTrue,
      UnaryFunction<BuilderT, BuilderT> applyFalse) {

    if (cond) {
      return applyTrue.apply((BuilderT) this);
    }
    return applyFalse.apply((BuilderT) this);
  }
}
