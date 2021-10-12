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

import org.apache.beam.sdk.annotations.Experimental;

/**
 * A factory for operations that execute a projection on a {@link Schema}-aware {@link
 * org.apache.beam.sdk.values.PCollection}.
 *
 * <p>Typically this interface will be implemented by a reader {@link
 * org.apache.beam.sdk.transforms.PTransform} that is capable of pushing down a projection to an
 * external source.
 */
@Experimental
public interface ProjectionProducer<T> {
  /** What kinds of projection support (if any) an operation offers. */
  enum ProjectSupport {
    /**
     * No projections are supported. In other words, the operation can only return a fixed set of
     * transforms from its input.
     */
    NONE,
    /**
     * Projections are supported as long as fields are projected in the same order as the data
     * source.
     */
    WITHOUT_FIELD_REORDERING,
    /** All projections are supported. */
    WITH_FIELD_REORDERING
  }

  /** What kinds of projection support (if any) this operation offers. Default: NONE */
  default ProjectSupport supportsProjectionPushdown() {
    return ProjectSupport.NONE;
  }

  /** Returns an operation that actuates the projection described by {@code fields}. */
  default T actuateProjectionPushdown(FieldAccessDescriptor fields) {
    throw new UnsupportedOperationException();
  }
}
