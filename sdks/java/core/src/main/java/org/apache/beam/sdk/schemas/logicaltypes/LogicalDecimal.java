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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/** A LogicalType representing a Decimal type with custom precision and scale. */
@Experimental(Experimental.Kind.SCHEMAS)
public class LogicalDecimal extends IdenticalBaseTAndInputTLogicalType<BigDecimal> {
  public static final String IDENTIFIER = "beam:logical_type:decimal:v1";
  private static final Schema schema =
      Schema.builder().addInt32Field("precision").addInt32Field("scale").build();
  private final int precision;
  private final int scale;

  private LogicalDecimal(int precision, int scale) {
    super(
        IDENTIFIER,
        Schema.FieldType.row(schema),
        Row.withSchema(schema).addValues(precision, scale).build(),
        Schema.FieldType.STRING);
    this.precision = precision;
    this.scale = scale;
  }

  public static LogicalDecimal of(int precision, int scale) {
    return new LogicalDecimal(precision, scale);
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  @Override
  public BigDecimal toBaseType(BigDecimal input) {
    checkArgument(input == null || (input.precision() == precision && input.scale() == scale));
    return input;
  }

  @Override
  public BigDecimal toInputType(BigDecimal base) {
    checkArgument(base == null || (base.precision() == precision && base.scale() == scale));
    return base;
  }
}
