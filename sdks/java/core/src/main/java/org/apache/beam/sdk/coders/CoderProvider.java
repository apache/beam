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

import java.util.List;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link CoderProvider} provides {@link Coder}s.
 *
 * <p>It may operate on a parameterized type, such as {@link List}, in which case the {@link
 * #coderFor} method accepts a list of coders to use for the type parameters.
 */
public abstract class CoderProvider {

  /**
   * Returns a {@code Coder<T>} to use for values of a particular type, given the Coders for each of
   * the type's generic parameter types.
   *
   * <p>Throws {@link CannotProvideCoderException} if this {@link CoderProvider} cannot provide a
   * coder for this type and components.
   */
  public abstract <T> Coder<T> coderFor(
      TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
      throws CannotProvideCoderException;
}
