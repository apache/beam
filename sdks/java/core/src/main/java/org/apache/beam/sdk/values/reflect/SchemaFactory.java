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

package org.apache.beam.sdk.values.reflect;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>Interface for factories used to create record types based on getters.
 *
 * <p>Different implementations can have different ways of mapping getter types to coders. For
 * example Beam SQL uses custom mapping via java.sql.Types.
 *
 * <p>Default implementation is {@link DefaultSchemaFactory}. It returns instances of {@link
 * Schema}, mapping {@link FieldValueGetter#type()} to known coders.
 */
@Internal
public interface SchemaFactory extends Serializable {

  /** Create a {@link Schema} for the list of the pojo field getters. */
  Schema createSchema(Iterable<FieldValueGetter> getters);
}
