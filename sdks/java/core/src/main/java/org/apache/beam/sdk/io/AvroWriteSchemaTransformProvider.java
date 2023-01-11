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
package org.apache.beam.sdk.io;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.joda.time.Duration;

public class AvroWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        AvroWriteSchemaTransformProvider.AvroWriteSchemaTransformConfiguration> {
  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:avro_write:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList("input");
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  protected Class<AvroWriteSchemaTransformConfiguration> configurationClass() {
    return AvroWriteSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(AvroWriteSchemaTransformConfiguration configuration) {
    return new AvroWriteSchemaTransform(configuration);
  }

  static class AvroWriteSchemaTransform implements SchemaTransform, Serializable {

    AvroWriteSchemaTransformConfiguration config;

    AvroWriteSchemaTransform(AvroWriteSchemaTransformConfiguration config) {
      this.config = config;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
          PCollection<GenericRecord> asRecords =
              input.get("input").apply("ToGenericRecords", Convert.to(GenericRecord.class));
          AvroIO.Write<GenericRecord> avroWrite =
              AvroIO.writeGenericRecords(AvroUtils.toAvroSchema(config.getDataSchema(), null, null))
                  .to(config.getLocation());
          if (input.get("input").isBounded() == PCollection.IsBounded.UNBOUNDED
              || config.getWriteWindowSizeSeconds() != null) {
            Long seconds = config.getWriteWindowSizeSeconds();
            if (seconds == null) {
              seconds = 60L;
            }
            Duration windowSize = Duration.standardSeconds(seconds);
            asRecords = asRecords.apply(Window.into(FixedWindows.of(windowSize)));
            avroWrite = avroWrite.withWindowedWrites().withNumShards(1);
          } else {
            avroWrite = avroWrite.withoutSharding();
          }
          asRecords.apply("AvroIOWrite", avroWrite);
          return PCollectionRowTuple.empty(asRecords.getPipeline());
        }
      };
    }
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class AvroWriteSchemaTransformConfiguration implements Serializable {
    public abstract String getLocation();

    public abstract Schema getDataSchema();

    @Nullable
    public abstract Long getWriteWindowSizeSeconds();

    public void validate() throws IllegalArgumentException {
      if (Strings.isNullOrEmpty(getLocation())) {
        throw new IllegalArgumentException("Location must be set for Avro writes.");
      }
      if (getDataSchema() == null || getDataSchema().getFields().size() == 0) {
        throw new IllegalArgumentException("Data schema has no fields.");
      }
    }

    public static Builder builder() {
      return new AutoValue_AvroWriteSchemaTransformProvider_AvroWriteSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setLocation(String value);

      public abstract Builder setDataSchema(Schema value);

      public abstract Builder setWriteWindowSizeSeconds(@Nullable Long value);

      public abstract AvroWriteSchemaTransformConfiguration build();
    }
  }
}
