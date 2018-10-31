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

import java.io.Serializable;
import java.lang.reflect.Type;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>An interface to set a field of a class.
 *
 * <p>Implementations of this interface are generated at runtime to map Row fields back into objet
 * fields.
 */
@Internal
public interface FieldValueSetter<ObjectT, ValueT> extends Serializable {
  /** Sets the specified field on object to value. */
  void set(ObjectT object, @Nullable ValueT value);

  /** Returns the name of the field. */
  String name();

  /** Returns the field type. */
  Class type();

  /** If the field is a container type, returns the element type. */
  @Nullable
  Type elementType();

  /** If the field is a map type, returns the key type. */
  @Nullable
  Type mapKeyType();

  /** If the field is a map type, returns the key type. */
  @Nullable
  Type mapValueType();
}
