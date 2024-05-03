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
package org.apache.beam.sdk.managed.testing;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

@AutoService(SchemaTransformProvider.class)
public class TestSchemaTransformProvider
    extends TypedSchemaTransformProvider<TestSchemaTransformProvider.Config> {
  private static final TestSchemaTransformProvider INSTANCE = new TestSchemaTransformProvider();
  public static final String IDENTIFIER = INSTANCE.identifier();
  public static final Schema SCHEMA = INSTANCE.configurationSchema();

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Config {
    public static Builder builder() {
      return new AutoValue_TestSchemaTransformProvider_Config.Builder();
    }

    @SchemaFieldDescription("String to add to each row element.")
    public abstract String getExtraString();

    @SchemaFieldDescription("Integer to add to each row element.")
    public abstract Integer getExtraInteger();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setExtraString(String extraString);

      public abstract Builder setExtraInteger(Integer extraInteger);

      public abstract Config build();
    }
  }

  @Override
  public SchemaTransform from(Config config) {
    String extraString = config.getExtraString();
    Integer extraInteger = config.getExtraInteger();
    return new SchemaTransform() {
      @Override
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        Schema schema =
            Schema.builder()
                .addFields(input.get("input").getSchema().getFields())
                .addStringField("extra_string")
                .addInt32Field("extra_integer")
                .build();
        PCollection<Row> rows =
            input
                .get("input")
                .apply(
                    MapElements.into(TypeDescriptors.rows())
                        .via(
                            row ->
                                Row.withSchema(schema)
                                    .addValues(row.getValues())
                                    .addValue(extraString)
                                    .addValue(extraInteger)
                                    .build()))
                .setRowSchema(schema);
        return PCollectionRowTuple.of("output", rows);
      }
    };
  }

  @Override
  public String identifier() {
    return "beam:test_schematransform:v1";
  }
}
