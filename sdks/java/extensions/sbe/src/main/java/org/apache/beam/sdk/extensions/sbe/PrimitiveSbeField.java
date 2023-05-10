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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.extensions.sbe.UnsignedOptions.Behavior;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import uk.co.real_logic.sbe.PrimitiveType;

/** Represents a primitive SBE field. */
@AutoValue
abstract class PrimitiveSbeField implements SbeField {
  private static final long serialVersionUID = 1L;

  @Override
  public abstract String name();

  @Override
  public abstract Boolean isRequired();

  public abstract PrimitiveType type();

  @Override
  public Field asBeamField(SbeFieldOptions options) {
    FieldType type = beamType(options);
    return isRequired() ? Field.of(name(), type) : Field.nullable(name(), type);
  }

  private FieldType beamType(SbeFieldOptions options) {
    switch (type()) {
      case CHAR:
        // TODO(https://github.com/apache/beam/issues/21102): Support char types
        throw new UnsupportedOperationException(
            "char type not supported yet (https://github.com/apache/beam/issues/21102)");
      case INT8:
        return FieldType.BYTE;
      case INT16:
        return FieldType.INT16;
      case INT32:
        return FieldType.INT32;
      case INT64:
        return FieldType.INT64;
      case FLOAT:
        return FieldType.FLOAT;
      case DOUBLE:
        return FieldType.DOUBLE;
      case UINT8:
        return convertUint8(options);
      case UINT16:
        return convertUint16(options);
      case UINT32:
        return convertUint32(options);
      case UINT64:
        return convertUint64(options);
      default:
        throw new IllegalStateException(
            "Got a state that is not recognized: " + type().primitiveName());
    }
  }

  private static FieldType convertUint8(SbeFieldOptions options) {
    Behavior behavior = options.unsignedOptions().uint8Behavior();
    switch (behavior) {
      case SAME_BIT_SIGNED:
        return FieldType.BYTE;
      case HIGHER_BIT_SIGNED:
        return FieldType.INT16;
      case CONVERT_TO_STRING:
        return FieldType.STRING;
      case CONVERT_TO_BIG_DECIMAL:
        return FieldType.DECIMAL;
      default:
        throw new IllegalStateException("Unrecognized uint8 behavior: " + behavior.name());
    }
  }

  private static FieldType convertUint16(SbeFieldOptions options) {
    Behavior behavior = options.unsignedOptions().uint16Behavior();
    switch (behavior) {
      case SAME_BIT_SIGNED:
        return FieldType.INT16;
      case HIGHER_BIT_SIGNED:
        return FieldType.INT32;
      case CONVERT_TO_STRING:
        return FieldType.STRING;
      case CONVERT_TO_BIG_DECIMAL:
        return FieldType.DECIMAL;
      default:
        throw new IllegalStateException("Unrecognized uint16 behavior: " + behavior.name());
    }
  }

  private static FieldType convertUint32(SbeFieldOptions options) {
    Behavior behavior = options.unsignedOptions().uint32Behavior();
    switch (behavior) {
      case SAME_BIT_SIGNED:
        return FieldType.INT32;
      case HIGHER_BIT_SIGNED:
        return FieldType.INT64;
      case CONVERT_TO_STRING:
        return FieldType.STRING;
      case CONVERT_TO_BIG_DECIMAL:
        return FieldType.DECIMAL;
      default:
        throw new IllegalStateException("Unrecognized uint32 behavior: " + behavior.name());
    }
  }

  private static FieldType convertUint64(SbeFieldOptions options) {
    Behavior behavior = options.unsignedOptions().uint64Behavior();
    switch (behavior) {
      case SAME_BIT_SIGNED:
        return FieldType.INT64;
      case HIGHER_BIT_SIGNED:
        throw new IllegalStateException(
            "Options say to use higher bit type, but that is impossible for 64-bit integers.");
      case CONVERT_TO_STRING:
        return FieldType.STRING;
      case CONVERT_TO_BIG_DECIMAL:
        return FieldType.DECIMAL;
      default:
        throw new IllegalStateException("Unrecognized uint64 behavior: " + behavior.name());
    }
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
