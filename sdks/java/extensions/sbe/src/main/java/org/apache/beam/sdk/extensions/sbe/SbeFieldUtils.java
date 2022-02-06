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

import org.apache.beam.sdk.extensions.sbe.SbeSchema.UnsignedOptions;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import uk.co.real_logic.sbe.PrimitiveType;

/** Utilities for working with SBE fields. */
final class SbeFieldUtils {
  private SbeFieldUtils() {}

  /**
   * Converts a SBE {@link PrimitiveType} to a Beam {@link FieldType}.
   *
   * <p>"Signed" in this case simply means that it isn't one of the unsigned types (uint8, uint16,
   * uint32, uint64). This includes the char type.
   *
   * @param sbePrimitive the signed {@link PrimitiveType}
   * @return the Beam equivalent of the SBE type
   */
  static FieldType signedSbePrimitiveToBeamPrimitive(PrimitiveType sbePrimitive) {
    switch (sbePrimitive) {
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
      default:
        throw new IllegalArgumentException("Type must be signed");
    }
  }

  /**
   * Converts a SBE {@link PrimitiveType} to a Beam {@link FieldType}.
   *
   * @param sbePrimitive the unsigned {@link PrimitiveType}
   * @param unsignedOptions options controlling which type to return
   * @return the equivalent Beam type or the next-higher-bit type if configured so in the options
   */
  static FieldType unsignedSbePrimitiveToBeamPrimitive(
      PrimitiveType sbePrimitive, UnsignedOptions unsignedOptions) {
    switch (sbePrimitive) {
      case UINT8:
        return unsignedOptions.useMoreBitsForUint8() ? FieldType.INT16 : FieldType.BYTE;
      case UINT16:
        return unsignedOptions.useMoreBitsForUint16() ? FieldType.INT32 : FieldType.INT16;
      case UINT32:
        return unsignedOptions.useMoreBitsForUint32() ? FieldType.INT64 : FieldType.INT32;
      case UINT64:
        return unsignedOptions.useStringForUint64() ? FieldType.STRING : FieldType.INT64;
      default:
        throw new IllegalArgumentException("Type must be unsigned");
    }
  }
}
