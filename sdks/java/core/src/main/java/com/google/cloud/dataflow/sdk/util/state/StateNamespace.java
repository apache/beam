/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util.state;

import java.io.IOException;

/**
 * A namespace used for scoping state stored with {@link StateInternals}.
 *
 * <p>Instances of {@code StateNamespace} are guaranteed to have a {@link #hashCode} and
 * {@link #equals} that uniquely identify the namespace.
 */
public interface StateNamespace {

  /**
   * Return a {@link String} representation of the key. It is guaranteed that this
   * {@code String} will uniquely identify the key.
   *
   * <p>This will encode the actual namespace as a {@code String}. It is
   * preferable to use the {@code StateNamespace} object when possible.
   *
   * <p>The string produced by the standard implementations will not contain a '+' character. This
   * enables adding a '+' between the actual namespace and other information, if needed, to separate
   * the two.
   */
  String stringKey();

  /**
   * Append the string representation of this key to the {@link Appendable}.
   */
  void appendTo(Appendable sb) throws IOException;

  /**
   * Return an {@code Object} to use as a key in a cache.
   *
   * <p>Different namespaces may use the same key in order to be treated as a unit in the cache.
   * The {@code Object}'s {@code hashCode} and {@code equals} methods will be used to determine
   * equality.
   */
  Object getCacheKey();
}
