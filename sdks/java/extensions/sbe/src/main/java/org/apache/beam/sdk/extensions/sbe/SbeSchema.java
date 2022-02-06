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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializerProvider;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import uk.co.real_logic.sbe.ir.Ir;

/**
 * Represents an SBE schema that can be translated to a Beam {@link Schema} and {@link
 * PayloadSerializerProvider}.
 *
 * <p>The schema represents a single SBE message. If the XML schema contains more than one message,
 * then a new instance must be created for each message that the pipeline will work with.
 *
 * <p>The currently supported ways of generating a schema are:
 *
 * <ul>
 *   <li>Through an intermediate representation ({@link Ir}).
 * </ul>
 *
 * <h3>Intermediate Representation</h3>
 *
 * <p>An {@link Ir} allows for a reflection-less way of getting a very accurate representation of
 * the SBE schema, since it is a tokenized form of the original XML schema. To help deal with some
 * ambiguities, such as which message to base the schema around, passing {@link IrOptions} is
 * required. See the Javadoc for the options for more details.
 *
 * <p>At this time, we cannot support serialization to an SBE message through IR. As a result, the
 * {@code byte[]} output from the {@link PayloadSerializerProvider} serializer will be from a JSON
 * representation of the message, not an SBE-serialized message. Downstream systems will need to
 * account for this.
 */
@Experimental(Kind.SCHEMAS)
public final class SbeSchema implements Serializable {
  private static final long serialVersionUID = 1L;

  private final @Nullable SerializableIr ir;
  private final @Nullable IrOptions irOptions;
  private final ImmutableList<SbeField> sbeFields;

  private SbeSchema(
      @Nullable SerializableIr ir,
      @Nullable IrOptions irOptions,
      ImmutableList<SbeField> sbeFields) {
    this.ir = ir;
    this.irOptions = irOptions;
    this.sbeFields = sbeFields;
  }

  /**
   * Creates a new {@link SbeSchema} from the given intermediate representation.
   *
   * <p>This makes no guarantees about the state of the returned instance. That is, it may or may
   * not have the generated SBE schema representation, and it may or may not have translated the SBE
   * schema into a Beam schema.
   *
   * @param ir the intermediate representation of the SBE schema. Modifications to the passed-in
   *     value will not be reflected in the returned instance.
   * @param irOptions options for configuring how to deal with cases where the desired behavior is
   *     ambiguous.
   * @return a new {@link SbeSchema} instance
   */
  public static SbeSchema fromIr(Ir ir, IrOptions irOptions) {
    ImmutableList<SbeField> sbeFields = IrFieldGenerator.generateFields(ir, irOptions);

    Ir copy =
        new Ir(
            ir.packageName(),
            ir.namespaceName(),
            ir.id(),
            ir.version(),
            ir.description(),
            ir.semanticVersion(),
            ir.byteOrder(),
            ImmutableList.copyOf(ir.headerStructure().tokens()));

    return new SbeSchema(SerializableIr.fromIr(copy), irOptions, sbeFields);
  }

  @VisibleForTesting
  @Nullable
  Ir getIr() {
    return ir == null ? null : ir.ir();
  }

  @VisibleForTesting
  @Nullable
  IrOptions getIrOptions() {
    return irOptions;
  }

  @VisibleForTesting
  ImmutableList<SbeField> getSbeFields() {
    return sbeFields;
  }

  /**
   * Options for controlling what to do with unsigned types, specifically whether to use a higher
   * bit count or, in the case of uint64, a string.
   */
  @AutoValue
  public abstract static class UnsignedOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    public abstract Boolean useMoreBitsForUint8();

    public abstract Boolean useMoreBitsForUint16();

    public abstract Boolean useMoreBitsForUint32();

    public abstract Boolean useStringForUint64();

    /**
     * Returns options for using the same bit size for all unsigned types.
     *
     * <p>This means that if an unsigned value from SBE comes in with a value outside the signed
     * range, then the negative equivalent (in terms of bits) will be used.
     */
    public static UnsignedOptions usingSameBitSize() {
      return UnsignedOptions.builder()
          .setUseMoreBitsForUint8(false)
          .setUseMoreBitsForUint16(false)
          .setUseMoreBitsForUint32(false)
          .setUseStringForUint64(false)
          .build();
    }

    /**
     * Returns options for using a higher bit count for unsigned types.
     *
     * <p>This means that if an unsigned value is encountered, it will always use the higher bit
     * count, even if that higher bit count is unnecessary. However, this means that if it is
     * necessary, then the proper value will be returned rather than the negative equivalent (in
     * terms of bits).
     *
     * <p>The {@code includeUint64} controls the behavior of 64-bit values, since no properly
     * higher-bit-numeric type exists. If true, then this will be converted into a string that must
     * be parsed if intended to be used as a number. Otherwise, it will still be a 64-bit type and
     * may return negative values.
     */
    public static UnsignedOptions usingHigherBitSize(boolean includeUint64) {
      return UnsignedOptions.builder()
          .setUseMoreBitsForUint8(true)
          .setUseMoreBitsForUint16(true)
          .setUseMoreBitsForUint32(true)
          .setUseStringForUint64(includeUint64)
          .build();
    }

    public static Builder builder() {
      return new AutoValue_SbeSchema_UnsignedOptions.Builder();
    }

    /** Builder for {@link UnsignedOptions}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setUseMoreBitsForUint8(Boolean value);

      public abstract Builder setUseMoreBitsForUint16(Boolean value);

      public abstract Builder setUseMoreBitsForUint32(Boolean value);

      public abstract Builder setUseStringForUint64(Boolean value);

      public abstract UnsignedOptions build();
    }
  }

  /**
   * Options for configuring schema generation from an {@link Ir}.
   *
   * <p>The default options make the following assumptions:
   *
   * <ul>
   *   <p>There is only message in the XML schema. In order to override this, either {@link
   *   IrOptions#messageId()} or {@link IrOptions#messageName()} must be set, but not both.
   * </ul>
   */
  @AutoValue
  public abstract static class IrOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final long UNSET_MESSAGE_ID = -1;
    public static final String UNSET_MESSAGE_NAME = "";

    public static final IrOptions DEFAULT = IrOptions.builder().build();

    public abstract long messageId();

    public abstract String messageName();

    public boolean assumeSingleMessageSchema() {
      return messageId() == UNSET_MESSAGE_ID && messageName().equals(UNSET_MESSAGE_NAME);
    }

    public static Builder builder() {
      return new AutoValue_SbeSchema_IrOptions.Builder()
          .setMessageId(UNSET_MESSAGE_ID)
          .setMessageName(UNSET_MESSAGE_NAME);
    }

    public abstract Builder toBuilder();

    /** Builder for {@link IrOptions}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setMessageId(long value);

      public abstract Builder setMessageName(String value);

      abstract IrOptions autoBuild();

      public IrOptions build() {
        IrOptions opts = autoBuild();

        boolean messageIdentifierValid =
            (opts.messageId() > UNSET_MESSAGE_ID ^ !opts.messageName().equals(UNSET_MESSAGE_NAME))
                || opts.assumeSingleMessageSchema();
        checkState(messageIdentifierValid, "At most one of messageId or messageName can be set");

        return opts;
      }
    }
  }
}
