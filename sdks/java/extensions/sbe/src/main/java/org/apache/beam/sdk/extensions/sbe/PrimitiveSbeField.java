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
package org.apache.beam.sdk.extensions.sbe;

import static org.apache.beam.sdk.extensions.sbe.SbeFieldUtils.signedSbePrimitiveToBeamPrimitive;
import static org.apache.beam.sdk.extensions.sbe.SbeFieldUtils.unsignedSbePrimitiveToBeamPrimitive;
import static uk.co.real_logic.sbe.PrimitiveType.isUnsigned;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import uk.co.real_logic.sbe.PrimitiveType;

/** Represents a primitive SBE field. */
@Experimental(Kind.SCHEMAS)
@AutoValue
public abstract class PrimitiveSbeField implements SbeField {
  private static final long serialVersionUID = 1L;

  @Override
  public abstract String name();

  @Override
  public abstract Boolean isRequired();

  public abstract PrimitiveType type();

  @Override
  public Field asBeamField(SbeFieldOptions options) {
    FieldType type =
        isUnsigned(type())
            ? unsignedSbePrimitiveToBeamPrimitive(type(), options.unsignedOptions())
            : signedSbePrimitiveToBeamPrimitive(type());
    return isRequired() ? Field.of(name(), type) : Field.nullable(name(), type);
  }

  public static Builder builder() {
    return new AutoValue_PrimitiveSbeField.Builder();
  }

  /** Builder for {@link PrimitiveSbeField}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setName(String value);

    public abstract Builder setIsRequired(Boolean value);

    public abstract Builder setType(PrimitiveType value);

    public abstract PrimitiveSbeField build();
  }
}
