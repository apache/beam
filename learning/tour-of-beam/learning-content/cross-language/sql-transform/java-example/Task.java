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

// beam-playground:
//   name: sql-transform
//   description: Sql transform
//   multifile: false
//   context_line: 33
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;

public class Task {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above
        Pipeline pipeline = Pipeline.create(options);

        Schema schema = Schema.builder()
                .addField("id", Schema.FieldType.INT32)
                .addField("name", Schema.FieldType.STRING)
                .build();

        PCollection<User> input = pipeline
                .apply(Create.of(new User(1, "Ab"), new User(103, "RE")))
                .setCoder(UserCoder.of())
                .setRowSchema(schema);

        PCollection<Row> result = input
                .apply(SqlTransform.query("SELECT id, name FROM PCOLLECTION"));

        result.apply(ParDo.of(new LogOutput<>()));

        pipeline.run();

    }

    static class UserCoder extends Coder<User> {
        private static final UserCoder INSTANCE = new UserCoder();

        public static UserCoder of() {
            return INSTANCE;
        }

        private static final String NAMES_SEPARATOR = "_";

        // So that the pipeline understands how encode
        @Override
        public void encode(User user, OutputStream outStream) throws IOException {
            String serializableRecord = user.id + NAMES_SEPARATOR + user.name;
            outStream.write(serializableRecord.getBytes());
        }

        // So that the pipeline understands how decode
        @Override
        public User decode(InputStream inStream) {
            String serializedRecord = new BufferedReader(new InputStreamReader(inStream)).lines()
                    .parallel().collect(Collectors.joining("\n"));
            String[] names = serializedRecord.split(NAMES_SEPARATOR);
            return new User(Integer.parseInt(names[0]), names[1]);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return null;
        }

        @Override
        public void verifyDeterministic() {
        }
    }

    @DefaultSchema(JavaFieldSchema.class)
    static class User {
        private Integer id;

        private String name;

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
        @SchemaCreate
        public User(Integer id, String name) {
            this.id = id;
            this.name = name;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}