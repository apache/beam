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
package org.apache.beam.sdk.schemas;

import java.util.Map;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A factory for operations that execute a projection on a {@link Schema}-aware {@link
 * org.apache.beam.sdk.values.PCollection}.
 *
 * <p>Typically this interface will be implemented by a reader {@link
 * org.apache.beam.sdk.transforms.PTransform} or some component thereof that is capable of pushing
 * down a projection to an external source.
 */
public interface ProjectionProducer<T> {
  /** Whether {@code this} supports projection pushdown. */
  boolean supportsProjectionPushdown();

  /**
   * Actuate a projection pushdown.
   *
   * @param outputFields Map keyed by the {@link TupleTag} for each output on which pushdown is
   *     requested. The value is the {@link FieldAccessDescriptor} containing the list of fields
   *     needed for that output; fields not present in the descriptor should be dropped.
   * @return {@code T} that implements the projection pushdown. The return value is assumed to be a
   *     drop-in replacement for {@code this}; it must have all the same functionality. For this
   *     reason, {@code T} is usually the same class as {@code this}.
   */
  T actuateProjectionPushdown(Map<TupleTag<?>, FieldAccessDescriptor> outputFields);
}
