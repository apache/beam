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
package org.apache.beam.sdk.schemas.io;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;

/**
 * Factory for creating a {@link PTransform} that can execute a projection.
 *
 * <p>Typically this interface will be implemented by a reader {@link PTransform} that is capable of
 * pushing down projection to an external source. For example, {@link SchemaIO#buildReader()} may
 * return a {@link PushdownProjector} to which a projection may be applied later.
 */
@Experimental
public interface PushdownProjector<InputT extends PInput> {
  /**
   * Returns a {@link PTransform} that will execute the projection specified by the {@link
   * FieldAccessDescriptor}.
   */
  PTransform<InputT, PCollection<Row>> withProjectionPushdown(
      FieldAccessDescriptor fieldAccessDescriptor);

  /**
   * Returns true if this instance can do a projection that returns fields in a different order than
   * the projection's inputs.
   */
  boolean supportsFieldReordering();
}
