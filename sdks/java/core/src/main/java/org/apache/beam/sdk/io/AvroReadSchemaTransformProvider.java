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

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;

@AutoService(SchemaTransformProvider.class)
public class AvroReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        AvroReadSchemaTransformProvider.AvroReadSchemaTransformConfiguration> {
  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:avro_read:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList("output");
  }

  @Override
  protected Class<AvroReadSchemaTransformConfiguration> configurationClass() {
    return AvroReadSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(AvroReadSchemaTransformConfiguration configuration) {
    return new AvroReadSchemaTransform(configuration);
  }

  static class AvroReadSchemaTransform extends PTransform<PCollectionRowTuple,PCollectionRowTuple> implements SchemaTransform {

    AvroReadSchemaTransformConfiguration config;

    AvroReadSchemaTransform(AvroReadSchemaTransformConfiguration config) {
      this.config = config;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return this;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      return PCollectionRowTuple.of(
              "output",
              input
                      .getPipeline()
                      .apply(
                              "AvroIORead",
                              AvroIO.readGenericRecords(
                                              AvroUtils.toAvroSchema(config.getDataSchema(), null, null))
                                      .withBeamSchemas(true)
                                      .from(config.getLocation()))
                      .apply("ToRows", Convert.toRows()));
    }
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class AvroReadSchemaTransformConfiguration implements Serializable {
    public abstract String getLocation();

    public abstract Schema getDataSchema();

    public void validate() throws IllegalArgumentException {
      if (Strings.isNullOrEmpty(getLocation())) {
        throw new IllegalArgumentException("Location must be set for Avro reads.");
      }
      if (getDataSchema() == null || getDataSchema().getFields().size() == 0) {
        throw new IllegalArgumentException("Data schema has no fields.");
      }
    }

    public static Builder builder() {
      return new AutoValue_AvroReadSchemaTransformProvider_AvroReadSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setLocation(String value);

      public abstract Builder setDataSchema(Schema schema);

      public abstract AvroReadSchemaTransformConfiguration build();
    }
  }
}
