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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.Serializable;

/**
 * Options for controlling what to do with unsigned types, specifically whether to use a higher bit
 * count or, in the case of uint64, a string.
 */
@AutoValue
public abstract class UnsignedOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final String UINT64_HIGHER_BIT_ERROR_MESSAGE =
      "There is no primitive with a higher bit count than 64 bits.";

  public abstract Behavior uint8Behavior();

  public abstract Behavior uint16Behavior();

  public abstract Behavior uint32Behavior();

  public abstract Behavior uint64Behavior();

  /**
   * Returns options for using the same bit size for all unsigned types.
   *
   * <p>This means that if an unsigned value from SBE comes in with a value outside the signed
   * range, then the negative equivalent (in terms of bits) will be used.
   */
  public static UnsignedOptions usingSameBitSize() {
    return UnsignedOptions.builder()
        .setUint8Behavior(Behavior.SAME_BIT_SIGNED)
        .setUint16Behavior(Behavior.SAME_BIT_SIGNED)
        .setUint32Behavior(Behavior.SAME_BIT_SIGNED)
        .setUint64Behavior(Behavior.SAME_BIT_SIGNED)
        .build();
  }

  /**
   * Returns options for using a higher bit count for unsigned types.
   *
   * <p>This means that if an unsigned value is encountered, it will always use the higher bit
   * count, even if that higher bit count is unnecessary. However, this means that if it is
   * necessary, then the proper value will be returned rather than the negative equivalent (in terms
   * of bits).
   *
   * <p>The {@code uint64Behavior} controls the behavior of 64-bit values, since no properly
   * higher-bit-numeric type exists. This cannot be {@link Behavior#HIGHER_BIT_SIGNED} or else an
   * exception will be thrown.
   */
  public static UnsignedOptions usingHigherBitSize(Behavior uint64Behavior) {
    checkArgument(uint64Behavior != Behavior.HIGHER_BIT_SIGNED, UINT64_HIGHER_BIT_ERROR_MESSAGE);
    return UnsignedOptions.builder()
        .setUint8Behavior(Behavior.HIGHER_BIT_SIGNED)
        .setUint16Behavior(Behavior.HIGHER_BIT_SIGNED)
        .setUint32Behavior(Behavior.HIGHER_BIT_SIGNED)
        .setUint64Behavior(uint64Behavior)
        .build();
  }

  public static Builder builder() {
    return new AutoValue_UnsignedOptions.Builder();
  }

  public abstract Builder toBuilder();

  /** Builder for {@link UnsignedOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setUint8Behavior(Behavior value);

    public abstract Builder setUint16Behavior(Behavior value);

    public abstract Builder setUint32Behavior(Behavior value);

    public abstract Builder setUint64Behavior(Behavior value);

    abstract UnsignedOptions autoBuild();

    public final UnsignedOptions build() {
      UnsignedOptions options = autoBuild();
      checkState(
          options.uint64Behavior() != Behavior.HIGHER_BIT_SIGNED, UINT64_HIGHER_BIT_ERROR_MESSAGE);
      return options;
    }
  }

  /** Defines the exact behavior for unsigned types. */
  public enum Behavior {
    /**
     * Uses the signed primitive with the same bit count.
     *
     * <p>If this option is chosen, unsigned types may appear to have negative values when printing
     * or doing math.
     *
     * <p>This option is compatible with all types.
     */
    SAME_BIT_SIGNED,

    /**
     * Uses the signed primitive with the next higher bit count.
     *
     * <p>If this option is chosen, then all unsigned fields will consume twice as much of the
     * available memory and networking bandwidth.
     *
     * <p>This option is incompatible with unsigned 64-bit types. Another option must be chosen for
     * that use case. If provided for 64-bit, then an exception will be thrown.
     */
    HIGHER_BIT_SIGNED,

    /**
     * Converts the unsigned value to a string representation.
     *
     * <p>This will be represented by a {@link org.apache.beam.sdk.schemas.Schema.FieldType#STRING}
     * in the schema.
     *
     * <p>This option is compatible with all types.
     */
    CONVERT_TO_STRING,

    /**
     * Converts the unsigned value to a {@link java.math.BigDecimal} value.
     *
     * <p>This will be represented by a {@link org.apache.beam.sdk.schemas.Schema.FieldType#DECIMAL}
     * in the schema.
     *
     * <p>this option is compatible with all types.
     */
    CONVERT_TO_BIG_DECIMAL
  }
}
