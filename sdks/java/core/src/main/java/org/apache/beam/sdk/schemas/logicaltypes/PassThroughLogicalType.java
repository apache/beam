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
package org.apache.beam.sdk.schemas.logicaltypes;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema.FieldType;

/** A base class for LogicalTypes that use the same value as the underlying base value. */
@Experimental(Kind.SCHEMAS)
public abstract class PassThroughLogicalType<T> extends IdenticalBaseTAndInputTLogicalType<T> {

  protected PassThroughLogicalType(
      String identifier, FieldType argumentType, Object argument, FieldType fieldType) {
    super(identifier, argumentType, argument, fieldType);
  }

  @Override
  public T toBaseType(T input) {
    return input;
  }

  @Override
  public T toInputType(T base) {
    return base;
  }
}
