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
package org.apache.beam.sdk.transforms.resourcehints;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Provides a definition of a resource hint known to the SDK. */
public abstract class ResourceHint {

  /**
   * Reconciles values of a hint when the hint specified on a transform is also defined in an outer
   * context, for example on a composite transform, or specified in the transform's execution
   * environment. Override this method for a custom reconciliation logic.
   */
  public ResourceHint mergeWithOuter(ResourceHint outer) {
    // Defaults to the inner value as it is the most specific one.
    return this;
  }

  /** Defines how to represent the as bytestring. */
  public abstract byte[] toBytes();

  @Override
  public abstract boolean equals(@Nullable Object other);

  @Override
  public abstract int hashCode();
}
