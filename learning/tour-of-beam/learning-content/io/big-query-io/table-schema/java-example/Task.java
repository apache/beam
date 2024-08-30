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
/*
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// beam-playground:
//   name: write-table-schema
//   description: BigQueryIO table-schema example.
//   multifile: false
//   context_line: 56
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import avro.shaded.com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    private static final String projectId = "project-id";
    private static final String dataset = "dataset";
    private static final String table = "table";

    public static void main(String[] args) {
        LOG.info("Running Task");
        System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", "to\\path\\credential.json");
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://bucket");
        options.as(BigQueryOptions.class).setProject(projectId);

        Pipeline pipeline = Pipeline.create(options);

        Schema inputSchema = Schema.builder()
                .addField("id", Schema.FieldType.INT32)
                .addField("name", Schema.FieldType.STRING)
                .addField("age", Schema.FieldType.INT32)
                .build();

        /*PCollection<User> pCollection = pipeline
                // Reads from the specified BigQuery table and maps the data to User objects.
                .apply(BigQueryIO.readTableRows()
                        .from(String.format("%s.%s.%s", projectId, dataset, table)))
                .apply(MapElements.into(TypeDescriptor.of(User.class)).via(it -> new User((String) it.get("id"), (String) it.get("name"), (Integer) it.get("age"))))
                .setCoder(CustomCoder.of())
                .setRowSchema(inputSchema);

        pCollection
                .apply("User", ParDo.of(new LogOutput<>()));
        */
        pipeline.run();
    }

    static class User {
        private String id;
        private String name;
        private Integer age;

        public User(String id, String name, Integer age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }

    static class CustomCoder extends Coder<User> {
        final ObjectMapper objectMapper = new ObjectMapper();
        private static final CustomCoder INSTANCE = new CustomCoder();

        public static CustomCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(User user, OutputStream outStream) throws IOException {
            String line = user.toString();
            outStream.write(line.getBytes());
        }

        @Override
        public User decode(InputStream inStream) throws IOException {
            final String serializedDTOs = new String(StreamUtils.getBytesWithoutClosing(inStream));
            return objectMapper.readValue(serializedDTOs, User.class);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() {
        }
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private final String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
        }
    }
}