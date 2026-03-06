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
package org.apache.beam.sdk.metrics;

import org.apache.beam.sdk.annotations.Internal;

/**
 * Plugin interface for lineage implementations.
 *
 * <p>This is the core contract that lineage plugins must implement. Plugins should implement this
 * interface and register via {@link org.apache.beam.sdk.lineage.LineageRegistrar}.
 *
 * <p>End users should use the {@link Lineage} facade class instead of implementing this interface
 * directly.
 */
@Internal
public interface LineageBase {
  /**
   * Adds the given FQN as lineage.
   *
   * @param rollupSegments should be an iterable of strings whose concatenation is a valid <a
   *     href="https://cloud.google.com/data-catalog/docs/fully-qualified-names">Dataplex FQN </a>
   *     which is already escaped.
   *     <p>In particular, this means they will often have trailing delimiters.
   */
  void add(Iterable<String> rollupSegments);
}
