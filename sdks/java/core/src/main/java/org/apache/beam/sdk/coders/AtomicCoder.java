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
package org.apache.beam.sdk.coders;

import java.util.Collections;
import java.util.List;

/**
 * A {@link Coder} that has no component {@link Coder Coders} or other state.
 *
 * <p>Note that, unless the behavior is overridden, atomic coders are presumed to be deterministic
 * and all instances are considered equal.
 *
 * @param <T> the type of the values being transcoded
 */
public abstract class AtomicCoder<T> extends StandardCoder<T> {
  protected AtomicCoder() { }

  @Override
  public void verifyDeterministic() throws NonDeterministicException { }

  @Override
  public final List<Coder<?>> getCoderArguments() {
    return null;
  }

  /**
   * Returns a list of values contained in the provided example
   * value, one per type parameter. If there are no type parameters,
   * returns an empty list.
   *
   * <p>Because {@link AtomicCoder} has no components, always returns an empty list.
   *
   * @param exampleValue unused, but part of the latent interface expected by
   * {@link CoderFactories#fromStaticMethods}
   */
  public static <T> List<Object> getInstanceComponents(T exampleValue) {
    return Collections.emptyList();
  }
}
