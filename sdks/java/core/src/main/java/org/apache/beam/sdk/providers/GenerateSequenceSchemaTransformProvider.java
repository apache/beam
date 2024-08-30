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
package org.apache.beam.sdk.providers;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.providers.GenerateSequenceSchemaTransformProvider.GenerateSequenceConfiguration;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

@AutoService(SchemaTransformProvider.class)
public class GenerateSequenceSchemaTransformProvider
    extends TypedSchemaTransformProvider<GenerateSequenceConfiguration> {
  public static final String OUTPUT_ROWS_TAG = "output";
  public static final Schema OUTPUT_SCHEMA = Schema.builder().addInt64Field("value").build();

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:generate_sequence:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_ROWS_TAG);
  }

  @Override
  public String description() {
    return String.format(
        "Outputs a PCollection of Beam Rows, each containing a single INT64 "
            + "number called \"value\". The count is produced from the given \"start\" "
            + "value and either up to the given \"end\" or until 2^63 - 1.%n"
            + "To produce an unbounded PCollection, simply do not specify an \"end\" value. "
            + "Unbounded sequences can specify a \"rate\" for output elements.%n"
            + "In all cases, the sequence of numbers is generated in parallel, so there is no "
            + "inherent ordering between the generated values");
  }

  @Override
  public Class<GenerateSequenceConfiguration> configurationClass() {
    return GenerateSequenceConfiguration.class;
  }

  @Override
  public SchemaTransform from(GenerateSequenceConfiguration configuration) {
    return new GenerateSequenceSchemaTransform(configuration);
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class GenerateSequenceConfiguration {
    @AutoValue
    public abstract static class Rate {
      @SchemaFieldDescription("Number of elements component of the rate.")
      public abstract Long getElements();

      @SchemaFieldDescription("Number of seconds component of the rate.")
      @Nullable
      public abstract Long getSeconds();

      public static Builder builder() {
        return new AutoValue_GenerateSequenceSchemaTransformProvider_GenerateSequenceConfiguration_Rate
            .Builder();
      }

      @AutoValue.Builder
      public abstract static class Builder {
        public abstract Builder setElements(Long elements);

        public abstract Builder setSeconds(Long seconds);

        public abstract Rate build();
      }
    }

    public static Builder builder() {
      return new AutoValue_GenerateSequenceSchemaTransformProvider_GenerateSequenceConfiguration
          .Builder();
    }

    @SchemaFieldDescription("The minimum number to generate (inclusive).")
    public abstract Long getStart();

    @SchemaFieldDescription(
        "The maximum number to generate (exclusive). Will be an unbounded sequence if left unspecified.")
    @Nullable
    public abstract Long getEnd();

    @SchemaFieldDescription(
        "Specifies the rate to generate a given number of elements per a given number of seconds. "
            + "Applicable only to unbounded sequences.")
    @Nullable
    public abstract Rate getRate();

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setStart(Long start);

      public abstract Builder setEnd(Long end);

      public abstract Builder setRate(Rate rate);

      public abstract GenerateSequenceConfiguration build();
    }

    public void validate() {
      checkNotNull(this.getStart(), "Must specify a starting point \"start\".");
      Long start = this.getStart();
      Long end = this.getEnd();
      if (end != null) {
        checkArgument(end == -1 || end >= start, "Invalid range [%s, %s)", start, end);
      }
      Rate rate = this.getRate();
      if (rate != null) {
        checkArgument(
            rate.getElements() > 0,
            "Invalid rate specification. Expected positive elements component but received %s.",
            rate.getElements());
        checkArgument(
            Optional.ofNullable(rate.getSeconds()).orElse(1L) > 0,
            "Invalid rate specification. Expected positive seconds component but received %s.",
            rate.getSeconds());
      }
    }
  }

  protected static class GenerateSequenceSchemaTransform extends SchemaTransform {
    private final GenerateSequenceConfiguration configuration;

    GenerateSequenceSchemaTransform(GenerateSequenceConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      checkArgument(
          input.getAll().isEmpty(), "Expected no inputs but got: %s", input.getAll().keySet());

      Long end = Optional.ofNullable(configuration.getEnd()).orElse(-1L);
      GenerateSequenceConfiguration.Rate rate = configuration.getRate();

      GenerateSequence sequence = GenerateSequence.from(configuration.getStart()).to(end);
      if (rate != null) {
        sequence =
            sequence.withRate(
                rate.getElements(),
                Duration.standardSeconds(Optional.ofNullable(rate.getSeconds()).orElse(1L)));
      }

      return PCollectionRowTuple.of(
          OUTPUT_ROWS_TAG,
          input
              .getPipeline()
              .apply(sequence)
              .apply(
                  MapElements.into(TypeDescriptors.rows())
                      .via(l -> Row.withSchema(OUTPUT_SCHEMA).withFieldValue("value", l).build()))
              .setRowSchema(OUTPUT_SCHEMA));
    }
  }
}
