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
//   name: rest-api-io
//   description: REST-API BigQueryIO example.
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
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
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

    private static final String projectId = "tess-372508";
    private static final String dataset = "fir";
    private static final String table = "xasw";

    public static void main(String[] args) {
        LOG.info("Running Task");
        System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", "C:\\Users\\menderes\\Downloads\\c.json");
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://btestq");
        options.as(BigQueryOptions.class).setProject(projectId);

        Pipeline pipeline = Pipeline.create(options);

        Schema inputSchema = Schema.builder()
                .addField("id", Schema.FieldType.INT32)
                .addField("name", Schema.FieldType.STRING)
                .addField("age", Schema.FieldType.INT32)
                .build();

        /*
        * The idea behind this code is to read data from a BigQuery table,
        * process it in some way (although the example provided doesn't perform any significant transformations beyond type conversion),
        * and then write the data back into BigQuery, but with a unique table for each user based on the user's "id".
        * */

        /*PCollection<User> pCollection = pipeline
                .apply(BigQueryIO.readTableRows()
                        .from(String.format("%s.%s.%s", projectId, dataset, table)))
                .apply(MapElements.into(TypeDescriptor.of(User.class)).via(it -> new User((String) it.get("id"), (String) it.get("name"), (Integer) it.get("age"))))
                .setCoder(CustomCoder.of())
                .setRowSchema(inputSchema);

        pCollection.apply(
                // Reading from BigQuery: The pipeline is configured to read data from a table in BigQuery, with the table's name, dataset, and project ID specified as variables. The data read is a collection of table rows.
                BigQueryIO.<User>write()
                        .to(
                                new DynamicDestinations<User, String>() {
                                    @Override
                                    public String getDestination(ValueInSingleWindow<User> elem) {
                                        return elem.getValue().id;
                                    }

                                    // The destination table schema is a list of three fields ("id", "name", and "age"), matching the fields of the User objects (as implemented in getSchema).
                                    @Override
                                    public TableDestination getTable(String destination) {
                                        return new TableDestination(
                                                new TableReference()
                                                        .setProjectId(projectId)
                                                        .setDatasetId(dataset)
                                                        .setTableId(table + "_" + destination),
                                                "Table for year " + destination);
                                    }

                                    @Override
                                    public TableSchema getSchema(String destination) {
                                        return new TableSchema()
                                                .setFields(
                                                        ImmutableList.of(
                                                                new TableFieldSchema()
                                                                        .setName("id")
                                                                        .setType("STRING")
                                                                        .setMode("REQUIRED"),
                                                                new TableFieldSchema()
                                                                        .setName("name")
                                                                        .setType("STRING")
                                                                        .setMode("REQUIRED"),
                                                                new TableFieldSchema()
                                                                        .setName("age")
                                                                        .setType("INTEGER")
                                                                        .setMode("REQUIRED")));
                                    }
                                })
                        .withFormatFunction(
                                (User elem) ->
                                        new TableRow()
                                                .set("id", elem.id)
                                                .set("name", elem.name)
                                                .set("age", elem.age))

//                    The write operation is configured to create the destination table if it does not already exist (CREATE_IF_NEEDED)
//                    and to replace any existing data in the destination table (WRITE_TRUNCATE).

                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));*/

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