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
package org.apache.beam.examples;

import com.google.auto.service.AutoService;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;

/**
 * An implementation of {@link SchemaTransformProvider} for reading and writing Avro files with
 * {@link AvroIO}.
 */
@Internal
@AutoService(SchemaTransformProvider.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class TextWriteTransformProvider implements SchemaTransformProvider {
  /** Returns an id that uniquely represents this IO. */
  @Override
  public String identifier() {
    return "text:write:v1";
  }

  /**
   * Returns the expected schema of the configuration object. Note this is distinct from the schema
   * of the data source itself. No configuration expected for Avro.
   */
  @Override
  public Schema configurationSchema() {
    return Schema.builder().addStringField("location").build(); // json, string
  }

  @Override
  public SchemaTransform from(Row configuration) {
    return new TextSchemaTransform(new Configuration(configuration));
  }

  @Override
  public List<String> getInputCollectionNames() {
    return Arrays.asList("input");
  }

  @Override
  public List<String> getOutputCollectionNames() {
    return Arrays.asList();
  }

  private static class Configuration {
    String location;

    public Configuration(Row row) {
      this.location = row.getString("location");
    }
  }

  /** An abstraction to create schema aware IOs. */
  private static class TextSchemaTransform implements SchemaTransform, Serializable {
    String location;

    private TextSchemaTransform(Configuration configuration) {
      location = configuration.location;
    }

    public static PCollection<Row> fromPCollectionTuple(PCollectionTuple input) {
      Collection<PCollection<?>> values = input.getAll().values();
      if (values.size() != 1) {
        throw new InvalidConfigurationException("");
      }
      return (PCollection<Row>) values.stream().iterator().next();
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
          PCollection<Row> rows = input.get("input");
          Schema schema = rows.getSchema();
          rows.apply(
                  "convert",
                  schema.getFieldCount() == 1
                          && schema.getField(0).getType().equals(FieldType.STRING)
                      ? Convert.fromRows(String.class)
                      : ToJson.of())
              .apply("TextIOWrite", TextIO.write().to(location).withoutSharding());
          return PCollectionRowTuple.empty(input.getPipeline());
        }
      };
    }
  }
}
