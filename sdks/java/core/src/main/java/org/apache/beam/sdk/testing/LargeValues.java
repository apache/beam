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

/**
 * Category tags for tests which validate that a Beam runner can handle values up to a given size.
 */
@Internal
public interface LargeValues {
  /** Tests if a runner supports 10KB keys. */
  interface KeysAbove10KB {}

  /** Tests if a runner supports 100KB keys. */
  interface KeysAbove100KB extends KeysAbove10KB {}

  /** Tests if a runner supports 1MB keys. */
  interface KeysAbove1MB extends KeysAbove100KB {}

  /** Tests if a runner supports 10MB keys. */
  interface KeysAbove10MB extends KeysAbove1MB {}

  /** Tests if a runner supports 100MB keys. */
  interface KeysAbove100MB extends KeysAbove10MB {}

  /** Tests if a runner supports iterables over 2GBs. */
  interface IterablesAbove2GB {}
}
