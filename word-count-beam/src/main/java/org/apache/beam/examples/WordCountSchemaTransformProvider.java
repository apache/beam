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
import java.util.Collections;
import java.util.List;
import org.apache.beam.examples.WordCount.WordCountTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

@AutoService(SchemaTransformProvider.class)
public class WordCountSchemaTransformProvider implements SchemaTransformProvider {
    public static abstract class SchemaTransformFront extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {}

    @Override
    public String identifier() {
        return "beam:schematransform:org.apache.beam:my_wordcount:v1";
    }

    @Override
    public Schema configurationSchema() {
        return Schema.builder().addStringField("inputFile").build();
    }

    @Override
    public List<String> inputCollectionNames() {
        return Collections.emptyList();
    }

    @Override
    public List<String> outputCollectionNames() {
        return Collections.singletonList("words");
    }

    @Override
    public SchemaTransform from(Row configuration) {
        return new SchemaTransform() {
            @Override
            public SchemaTransformFront buildTransform() {
                return new WordCountSchemaTransform(configuration);
            }
        };
    }

    public static class WordCountSchemaTransform extends SchemaTransformFront {
        Row configuration;

        WordCountSchemaTransform(Row configuration) {
            this.configuration = configuration;
        }

        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
            Schema schema = Schema.builder().addStringField("wordCount").build();
            PCollection<Row> wordCountRows =
                input
                    .getPipeline()
                    .apply(new WordCountTransform(configuration.getValue("inputFile")))
                    .apply(
                        "To Beam Rows",
                        MapElements.into(TypeDescriptors.rows())
                            .via(
                                wordCount ->
                                    Row.withSchema(schema)
                                        .withFieldValue("wordCount", wordCount)
                                        .build()))
                    .setRowSchema(schema);

            return PCollectionRowTuple.of("words", wordCountRows);
        }
    }

    public static void main(String[] args) {
        System.out.println("hi im here");
    }
}
