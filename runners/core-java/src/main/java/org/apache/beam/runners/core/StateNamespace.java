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
package org.apache.beam.runners.core;

import java.io.IOException;

/**
 * A namespace used for scoping state stored with {@link StateInternals}.
 *
 * <p>Instances of {@link StateNamespace} are guaranteed to have a {@link Object#hashCode} and
 * {@link Object#equals} that uniquely identify the namespace.
 */
public interface StateNamespace {

  /**
   * Return a {@link String} representation of the key. It is guaranteed that this {@link String}
   * will uniquely identify the key.
   *
   * <p>This will encode the actual namespace as a {@link String}. It is preferable to use the
   * {@link StateNamespace} object when possible.
   *
   * <p>The string produced by the standard implementations will not contain a '+' character. This
   * enables adding a '+' between the actual namespace and other information, if needed, to separate
   * the two.
   */
  String stringKey();

  /** Append the string representation of this key to the {@link Appendable}. */
  void appendTo(Appendable sb) throws IOException;

  /**
   * Return an {@link Object} to use as a key in a cache.
   *
   * <p>Different namespaces may use the same key in order to be treated as a unit in the cache. The
   * {@link Object}'s {@link Object#hashCode} and {@link Object#equals} methods will be used to
   * determine equality.
   */
  Object getCacheKey();
}
