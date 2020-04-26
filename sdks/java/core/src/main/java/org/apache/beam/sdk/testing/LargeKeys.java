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
package org.apache.beam.sdk.testing;

import org.apache.beam.sdk.annotations.Internal;

/** Category tags for tests which validate that a Beam runner can handle keys up to a given size. */
@Internal
public interface LargeKeys {
  /** Tests if a runner supports 10KB keys. */
  interface Above10KB {}

  /** Tests if a runner supports 100KB keys. */
  interface Above100KB extends Above10KB {}

  /** Tests if a runner supports 1MB keys. */
  interface Above1MB extends Above100KB {}

  /** Tests if a runner supports 10MB keys. */
  interface Above10MB extends Above1MB {}

  /** Tests if a runner supports 100MB keys. */
  interface Above100MB extends Above10MB {}
}
