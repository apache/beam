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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import uk.co.real_logic.sbe.ir.Ir;

/**
 * Represents an SBE schema.
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
 * required.
 */
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
    Ir copy = createIrCopy(ir);
    return new SbeSchema(SerializableIr.fromIr(copy), irOptions, sbeFields);
  }

  public @Nullable Ir getIr() {
    return ir == null ? null : createIrCopy(ir.ir());
  }

  public @Nullable IrOptions getIrOptions() {
    return irOptions;
  }

  public ImmutableList<SbeField> getSbeFields() {
    return sbeFields;
  }

  private static Ir createIrCopy(Ir ir) {
    return new Ir(
        ir.packageName(),
        ir.namespaceName(),
        ir.id(),
        ir.version(),
        ir.description(),
        ir.semanticVersion(),
        ir.byteOrder(),
        ImmutableList.copyOf(ir.headerStructure().tokens()));
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

    public static final IrOptions DEFAULT = IrOptions.builder().build();

    public abstract @Nullable Long messageId();

    public abstract @Nullable String messageName();

    public boolean assumeSingleMessageSchema() {
      return messageId() == null && messageName() == null;
    }

    public static Builder builder() {
      return new AutoValue_SbeSchema_IrOptions.Builder().setMessageId(null).setMessageName(null);
    }

    public abstract Builder toBuilder();

    /** Builder for {@link IrOptions}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setMessageId(@Nullable Long value);

      public abstract Builder setMessageName(@Nullable String value);

      abstract IrOptions autoBuild();

      public IrOptions build() {
        IrOptions opts = autoBuild();

        boolean messageIdentifierValid =
            (opts.messageId() != null ^ opts.messageName() != null)
                || opts.assumeSingleMessageSchema();
        checkState(messageIdentifierValid, "At most one of messageId or messageName can be set");

        return opts;
      }
    }
  }
}
