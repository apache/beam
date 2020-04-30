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

import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;

/** A base class for LogicalTypes that use the same input type as the underlying base type. */
@Experimental(Experimental.Kind.SCHEMAS)
public abstract class IdenticalBaseTAndInputTLogicalType<T> implements Schema.LogicalType<T, T> {
  protected final String identifier;
  protected final Schema.FieldType argumentType;
  protected final Object argument;
  protected final Schema.FieldType baseType;

  protected IdenticalBaseTAndInputTLogicalType(
      String identifier,
      Schema.FieldType argumentType,
      Object argument,
      Schema.FieldType baseType) {
    this.identifier = identifier;
    this.argumentType = argumentType;
    this.argument = argument;
    this.baseType = baseType;
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public Schema.FieldType getArgumentType() {
    return argumentType;
  }

  @Override
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <ArgumentT> ArgumentT getArgument() {
    return (ArgumentT) argument;
  }

  @Override
  public Schema.FieldType getBaseType() {
    return baseType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IdenticalBaseTAndInputTLogicalType<?> that = (IdenticalBaseTAndInputTLogicalType<?>) o;
    return identifier.equals(that.identifier)
        && argument.equals(that.argument)
        && baseType.equals(that.baseType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(identifier, argument, baseType);
  }
}
