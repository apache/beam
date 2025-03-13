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

/**
 * A {@link ProjectionConsumer} is a {@link Schema}-aware operation (such as a {@link
 * org.apache.beam.sdk.transforms.DoFn} or {@link org.apache.beam.sdk.transforms.PTransform}) that
 * has a {@link org.apache.beam.sdk.schemas.FieldAccessDescriptor} describing which fields the
 * operation accesses.
 */
public interface ProjectionConsumer {
  /**
   * Returns a map from input {@link org.apache.beam.sdk.values.TupleTag} id to a {@link
   * org.apache.beam.sdk.schemas.FieldAccessDescriptor} describing which Schema fields {@code this}
   * must access from the corresponding input {@link org.apache.beam.sdk.values.PCollection} to
   * complete successfully. Fields not listed in the {@link
   * org.apache.beam.sdk.schemas.FieldAccessDescriptor} are assumed to be safe to drop from the
   * input. Input {@link org.apache.beam.sdk.values.PCollection}s not found in the keyset are
   * implied to have value {@link FieldAccessDescriptor#withAllFields()}.
   */
  Map<String, FieldAccessDescriptor> consumesProjection();
}
