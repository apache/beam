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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A representation of an SBE schema that translates to a Beam schema.
 *
 * <p>In constructing the schema, it is necessary that field order is preserved relative to the SBE
 * XML schema. This is to make sure that we remain compatible with any implementations that attempt
 * to read or write the SBE message in a single, forward-only pass.
 */
@Experimental(Kind.SCHEMAS)
public final class SbeSchema {

  private final Schema schema;

  private SbeSchema(Schema schema) {
    this.schema = schema;
  }

  /**
   * Creates a new {@link Builder}.
   *
   * <p>This builder will not do any schema generation. The resulting {@link SbeSchema} will only
   * contain the fields that are provided to the builder.
   */
  static Builder builder() {
    return new Builder();
  }

  // TODO(zhoufek): Add factory methods for stubs, IR, and XML

  public Schema schema() {
    return schema;
  }

  // TODO(BEAM-12697): Add PayloadSerializer

  /** Builder for {@link SbeSchema}. */
  public static final class Builder {
    // TODO(BEAM-12697): Add fields for stubs, IR, and XML

    private final List<@Nullable Field> fields;

    private Builder() {
      this.fields = new ArrayList<>();
    }

    /**
     * Adds the field.
     *
     * <p>This field will not be evaluated during schema generation. It is assumed that it is in the
     * correct position.
     *
     * @param field the field to add
     */
    public Builder addField(Field field) {
      fields.add(field);
      return this;
    }

    /**
     * Inserts the field at the specified position.
     *
     * <p>This field will not be evaluated during schema generation. It is assumed that it is in the
     * correct position.
     *
     * <p>If {@code position} is greater than the current size, then nulls will be inserted. It is
     * assumed that these will be filled in during schema generation. If any are still null after
     * the remaining fields are filled in, then an {@link IllegalStateException} will be thrown
     * during {@link Builder#build()}.
     *
     * @param position the position to insert the field at
     * @param field the field to insert
     */
    public Builder insertField(int position, Field field) {
      for (int i = fields.size(); i < position; ++i) {
        fields.add(null);
      }
      fields.add(position, field);
      return this;
    }

    /**
     * Returns the field at index {@code i}.
     *
     * <p>This will throw a {@link IllegalStateException} if the field at {@code i} is null.
     */
    private @Initialized @NonNull Field getField(int i) {
      Field field = fields.get(i);
      if (field == null) {
        throw new IllegalStateException("Field at index " + i + " was never set.");
      }
      return field;
    }

    /**
     * Creates the {@link SbeSchema}.
     *
     * <p>If anything was provided for custom schema generation, that will be done on the call to
     * this method. If fields were inserted in such a way that nulls filled in empty space, such as
     * through {@link Builder#insertField(int, Field)}, then the remaining fields must be filled in
     * by the time that this generation completes. Otherwise, an {@link IllegalStateException} will
     * be thrown.
     */
    public SbeSchema build() {
      // TODO(BEAM-12697): Generate the rest of the fields.

      Schema.Builder schema = Schema.builder();
      IntStream.range(0, fields.size()).mapToObj(this::getField).forEach(schema::addField);

      return new SbeSchema(schema.build());
    }
  }
}
