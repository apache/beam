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
//   name: write-beam-schema
//   description: BiqQueryIO beam-schema example.
//   multifile: false
//   context_line: 56
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        LOG.info("Running Task");
        System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", "to\\path\\credential.json");
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://bucket");
        options.as(BigQueryOptions.class).setProject("project-id");

        Pipeline pipeline = Pipeline.create(options);

        Schema inputSchema = Schema.builder()
                .addField("id", Schema.FieldType.INT32)
                .addField("name", Schema.FieldType.STRING)
                .addField("age", Schema.FieldType.INT32)
                .build();

        /*
        PCollection<Object> pCollection = pipeline
        // The pipeline is reading data from a BigQuery table called "table" that's in the dataset "dataset" from the project with the ID "project-id". The data read is a collection of table rows.
                .apply(BigQueryIO.readTableRows()
                        .from("project-id.dataset.table"))
                .apply(MapElements.into(TypeDescriptor.of(Object.class)).via(it -> it))
                .setCoder(CustomCoder.of())
        // The setRowSchema(inputSchema) is used to provide the schema of the rows in the PCollection. This is necessary for some Beam operations, including writing to BigQuery using Beam schema.
                .setRowSchema(inputSchema);

        // The useBeamSchema() method indicates that the schema for the table is to be inferred from the types of the elements in the PCollection.
        pCollection
                .apply("WriteToBigQuery", BigQueryIO.write()
                        .to("mydataset.outputtable")
                        .useBeamSchema());
        */
        pipeline.run();
    }

    static class CustomCoder extends Coder<Object> {
        final ObjectMapper objectMapper = new ObjectMapper();
        private static final CustomCoder INSTANCE = new CustomCoder();

        public static CustomCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(Object user, OutputStream outStream) throws IOException {
            String line = user.toString();
            outStream.write(line.getBytes());
        }

        @Override
        public Object decode(InputStream inStream) throws IOException {
            final String serializedDTOs = new String(StreamUtils.getBytesWithoutClosing(inStream));
            return objectMapper.readValue(serializedDTOs, Object.class);
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