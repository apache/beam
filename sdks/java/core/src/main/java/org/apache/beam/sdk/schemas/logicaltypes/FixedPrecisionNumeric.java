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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.math.MathContext;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;

/** Fixed precision numeric types used to represent jdbc NUMERIC and DECIMAL types. */
public class FixedPrecisionNumeric extends PassThroughLogicalType<BigDecimal> {
  // TODO(https://github.com/apache/beam/issues/23373) promote this URN to schema.proto once logical
  // types with arguments are fully supported and the implementation of this logical type can thus
  // be considered standardized.
  public static final String IDENTIFIER = "beam:logical_type:fixed_decimal:v1";

  // TODO(https://github.com/apache/beam/issues/23374) implement beam:logical_type:decimal:v1 as
  // CoderLogicalType (once CoderLogicalType is implemented).
  /**
   * Identifier of the unspecified precision numeric type. It corresponds to Java SDK's {@link
   * FieldType#DECIMAL}. It is the underlying representation type of FixedPrecisionNumeric logical
   * type in order to be compatible with existing Java field types.
   */
  public static final String BASE_IDENTIFIER;

  private static final Schema SCHEMA;

  static {
    BASE_IDENTIFIER =
        SchemaApi.LogicalTypes.Enum.DECIMAL
            .getValueDescriptor()
            .getOptions()
            .getExtension(RunnerApi.beamUrn);
    SCHEMA = Schema.builder().addInt32Field("precision").addInt32Field("scale").build();
  }

  private final int precision;
  private final int scale;

  /**
   * Create a FixedPrecisionNumeric instance with specified precision and scale. ``precision=-1``
   * indicates unspecified precision.
   */
  public static FixedPrecisionNumeric of(int precision, int scale) {
    return new FixedPrecisionNumeric(precision, scale);
  }

  /** Create a FixedPrecisionNumeric instance with specified scale and unspecified precision. */
  public static FixedPrecisionNumeric of(int scale) {
    return of(-1, scale);
  }

  /** Create a FixedPrecisionNumeric instance with specified argument row. */
  public static FixedPrecisionNumeric of(Row row) {
    checkArgument(
        row.getSchema().assignableTo(SCHEMA),
        "Row has an incompatible schema to construct the logical type object: %s",
        row.getSchema());

    final Integer precision = row.getInt32("precision");
    final Integer scale = row.getInt32("scale");
    checkArgument(
        precision != null && scale != null,
        "precision and scale cannot be null for FixedPrecisionNumeric arguments.");
    // firstNonNull is used to cast precision and scale to @NonNull input
    return of(firstNonNull(precision, -1), firstNonNull(scale, 0));
  }

  private FixedPrecisionNumeric(int precision, int scale) {
    super(
        IDENTIFIER,
        FieldType.row(SCHEMA),
        Row.withSchema(SCHEMA).addValues(precision, scale).build(),
        FieldType.DECIMAL);
    this.precision = precision;
    this.scale = scale;
  }

  @Override
  public BigDecimal toInputType(BigDecimal base) {
    if (precision != -1) {
      // check value not causing overflow when precision is fixed.
      checkArgument(
          base == null
              || (base.precision() <= precision && base.scale() <= scale)
              || base.round(new MathContext(precision)).compareTo(base) == 0,
          "Expected BigDecimal base to be null or have precision <= %s (was %s), scale <= %s (was %s)",
          precision,
          (base == null) ? null : base.precision(),
          scale,
          (base == null) ? null : base.scale());
    }
    return base;
  }
}
