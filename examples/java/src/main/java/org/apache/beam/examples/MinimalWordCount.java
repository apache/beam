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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaUtils;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

public class MinimalWordCount {
  public static class TransformSpec {
    public String inputId;
    public List<Object> configurationAsList = null; // Or represent as objects.
    public Row configurationAsRow = null;

    public TransformSpec(String inputId, Row configurationAsRow) {
      this.inputId = inputId;
      this.configurationAsRow = configurationAsRow;
    }

    public TransformSpec(String inputId, List<Object> configurationAsList) {
      this.inputId = inputId;
      this.configurationAsList = configurationAsList;
    }

    private SchemaTransformProvider getProvider() {
      // Check for transform provider:
      ServiceLoader<SchemaTransformProvider> provider =
          ServiceLoader.load(SchemaTransformProvider.class);

      Optional<SchemaTransformProvider> maybeProvider =
          StreamSupport.stream(provider.spliterator(), false)
              .filter(p -> p.identifier().equals(inputId))
              .findFirst();
      if (maybeProvider.isPresent()) {
        return maybeProvider.get();
      }

      // Try to look for name:read.
      int lastIndex = inputId.lastIndexOf(":");
      String inputName = inputId.substring(0, lastIndex);
      boolean isRead = inputId.substring(lastIndex + 1).equals("read");

      SchemaIOProvider ioProvider =
          StreamSupport.stream(ServiceLoader.load(SchemaIOProvider.class).spliterator(), false)
              .filter(p -> p.identifier().equals(inputName))
              .findFirst()
              .get();
      return new SchemaIOToTransformProvider(ioProvider, isRead);
    }

    public SchemaTransform getSchemaTransform() {
      SchemaTransformProvider provider = getProvider();
      return provider.from(
          configurationAsRow == null
              ? Row.withSchema(provider.configurationSchema())
                  .addValues(configurationAsList)
                  .build()
              : SchemaUtils.convertRowToSchema(configurationAsRow, provider.configurationSchema()));
    }
  }

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    boolean single = false;
    boolean avroInput = true;
    Schema schema =
        single
            ? Schema.of(Field.of("single", FieldType.STRING))
            : Schema.of(Field.of("species", FieldType.STRING), Field.of("number", FieldType.INT32));

    /*
    ArrayList<Row> records = new ArrayList<>();
    if (single) {
      records.add(Row.withSchema(schema).addValues("dog", 5).build());
      records.add(Row.withSchema(schema).addValues("cat", 9).build());
      records.add(Row.withSchema(schema).addValues("fish", 15).build());
    } else {
      records.add(Row.withSchema(schema).addValue("dog").build());
      records.add(Row.withSchema(schema).addValue("cat").build());
      records.add(Row.withSchema(schema).addValue("frog").build());
    }
    p.apply(Create.of(records).withCoder(RowCoder.of(schema))).apply(((SchemaIO) new AvroSchemaIOProvider().from("avro-input*", null, schema)).buildWriter());
    */

    FieldDropperTransformProvider provider;

    String filePrefix = "/Users/laraschmidt/Documents/beam2/files/";

    ArrayList<TransformSpec> specs = new ArrayList<>();
    if (avroInput) {
      String filename = filePrefix + (single ? "avro-single-input*" : "avro-input*");
      specs.add(new TransformSpec("avro:read", Arrays.asList(schema, filename)));
    } else {
      Row textConfig =
          Row.withSchema(Schema.builder().addStringField("location").build())
              .addValue(filePrefix + "text")
              .build();
      specs.add(new TransformSpec("text:read:v1", textConfig));
    }
    if (!single) {
      specs.add(new TransformSpec("fieldDropper", Arrays.asList("number")));
    }
    specs.add(new TransformSpec("text:write:v1", Arrays.asList("text-out")));

    PCollectionRowTuple tuple = PCollectionRowTuple.empty(p);
    Map<String, Schema> schemaMap = new HashMap<>();

    for (TransformSpec spec : specs) {
      System.out.println(spec.inputId);
      SchemaTransform transform = spec.getSchemaTransform();
      // We know we only deal with transforms with either 0 or 1 input so we know how to connect the
      // collections. To sanity check we should confirm the output collections match expected.
      if (tuple.getAll().size() == 1) {
        String input = spec.getProvider().getInputCollectionNames().get(0);
        String priorOutput = tuple.getAll().keySet().stream().findFirst().get().getId();
        tuple = PCollectionRowTuple.of(input, tuple.get(priorOutput));
        schemaMap.put(input, schemaMap.remove(priorOutput));
      }

      tuple = tuple.apply(transform.buildTransform());
      schemaMap = SchemaUtils.getSchema(transform, schemaMap);
    }

    p.run().waitUntilFinish();
  }
}
