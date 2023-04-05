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
package org.apache.beam.sdk.extensions.euphoria.core.client.type;

import java.util.Optional;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Utility methods related to type awareness. */
public class TypeAwareness {

  private static final TypeDescriptor<Object> OBJECT_TYPE = new TypeDescriptor<Object>() {};

  @SuppressWarnings("unchecked")
  public static <T> TypeDescriptor<T> orObjects(Optional<TypeDescriptor<T>> maybeType) {
    if (maybeType.isPresent() && !maybeType.get().hasUnresolvedParameters()) {
      return maybeType.get();
    }
    return (TypeDescriptor) OBJECT_TYPE;
  }
}
