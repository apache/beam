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
//   name: SchemaBasedSolution
//   description: Schema Based Solution example.
//   multifile: false
//   context_line: 137
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.io.Serializable;

public class Task {
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    @DefaultSchema(JavaFieldSchema.class)
    public static class Game {
        public String userId;
        public Integer score;
        public String gameId;
        public String date;

        @SchemaCreate
        public Game(String userId, Integer score, String gameId, String date) {
            this.userId = userId;
            this.score = score;
            this.gameId = gameId;
            this.date = date;
        }

        @Override
        public String toString() {
            return "Game{" +
                    "userId='" + userId + '\'' +
                    ", score='" + score + '\'' +
                    ", gameId='" + gameId + '\'' +
                    ", date='" + date + '\'' +
                    '}';
        }
    }

    // User schema
    @DefaultSchema(JavaFieldSchema.class)
    public static class User {

        public String userId;
        public String userName;

        @SchemaCreate
        public User(String userId, String userName) {
            this.userId = userId;
            this.userName = userName;
        }

        @Override
        public String toString() {
            return "User{" +
                    "userId='" + userId + '\'' +
                    ", userName='" + userName + '\'' +
                    '}';
        }
    }


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<User> userInfo = getUserPCollection(pipeline);
        PCollection<Game> gameInfo = getGamePCollection(pipeline);

        Schema userSchema = Schema.builder()
                .addStringField("userId")
                .addStringField("userName")
                .build();

        PCollection<Row> pCollection = userInfo
                .setSchema(userSchema,
                        TypeDescriptor.of(User.class), input ->
                        {
                            User user = input;
                            return Row.withSchema(userSchema)
                                    .addValues(user.userId, user.userName)
                                    .build();
                        },
                        input -> new User(input.getString(0), input.getString(1))
                )
                .apply(Join.<User,Game>innerJoin(gameInfo).using("userId"))
                .apply(Group.<Row>byFieldNames("lhs.userId").aggregateField("rhs.score", Sum.ofIntegers(), "total"))
                .apply(Filter.<Row>create().whereFieldName("value.total", s -> (int) s > 11));

        pCollection
                .apply("User", ParDo.of(new LogOutput<>("Result")));

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

    public static PCollection<User> getUserPCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/game/small/gaming_data.csv"));
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);
        return rides.apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new ExtractUserFn()));
    }

    public static PCollection<Game> getGamePCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/game/small/gaming_data.csv"));
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);
        return rides.apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new ExtractUserProgressFn()));
    }

    static class ExtractUserFn extends DoFn<String, User> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            c.output(new User(items[0], items[1]));
        }
    }

    static class ExtractUserProgressFn extends DoFn<String, Game> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            c.output(new Game(items[0], Integer.valueOf(items[2]), items[3], items[4]));
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
