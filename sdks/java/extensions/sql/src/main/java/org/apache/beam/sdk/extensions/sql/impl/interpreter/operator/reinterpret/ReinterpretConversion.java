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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.reinterpret;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;

/** Defines conversion between 2 SQL types. */
public class ReinterpretConversion {

  /** Builder for {@link ReinterpretConversion}. */
  public static class Builder {

    private Set<SqlTypeName> from = new HashSet<>();
    private SqlTypeName to;
    private Function<BeamSqlPrimitive, BeamSqlPrimitive> convert;

    public Builder from(SqlTypeName from) {
      this.from.add(from);
      return this;
    }

    public Builder from(Collection<SqlTypeName> from) {
      this.from.addAll(from);
      return this;
    }

    public Builder from(SqlTypeName... from) {
      return from(Arrays.asList(from));
    }

    public Builder to(SqlTypeName to) {
      this.to = to;
      return this;
    }

    public Builder convert(Function<BeamSqlPrimitive, BeamSqlPrimitive> convert) {
      this.convert = convert;
      return this;
    }

    public ReinterpretConversion build() {
      if (from.isEmpty() || to == null || convert == null) {
        throw new IllegalArgumentException(
            "All arguments to ReinterpretConversion.Builder" + " are mandatory.");
      }
      return new ReinterpretConversion(this);
    }
  }

  private Set<SqlTypeName> from;
  private SqlTypeName to;
  private Function<BeamSqlPrimitive, BeamSqlPrimitive> convertFunction;

  private ReinterpretConversion(Builder builder) {
    this.from = ImmutableSet.copyOf(builder.from);
    this.to = builder.to;
    this.convertFunction = builder.convert;
  }

  public static Builder builder() {
    return new Builder();
  }

  public BeamSqlPrimitive convert(BeamSqlPrimitive input) {
    if (!from.contains(input.getOutputType())) {
      throw new IllegalArgumentException(
          "Unable to convert from "
              + input.getOutputType().name()
              + " to "
              + to.name()
              + ". This conversion only supports "
              + toString());
    }

    return convertFunction.apply(input);
  }

  public SqlTypeName to() {
    return to;
  }

  public Set<SqlTypeName> from() {
    return from;
  }

  @Override
  public String toString() {
    return from.toString() + "->" + to.name();
  }
}
