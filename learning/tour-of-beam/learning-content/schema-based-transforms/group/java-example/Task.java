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
//   name: group
//   description: Group example.
//   multifile: false
//   context_line: 46
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;


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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Game game = (Game) o;
            return Objects.equals(userId, game.userId) && Objects.equals(score, game.score) && Objects.equals(gameId, game.gameId) && Objects.equals(date, game.date);
        }

        @Override
        public int hashCode() {
            return Objects.hash(userId, score, gameId, date);
        }
    }

    // User schema
    @DefaultSchema(JavaFieldSchema.class)
    public static class User {

        public String userId;
        public String userName;
        public Game game;

        @SchemaCreate
        public User(String userId, String userName
                , Game game
        ) {
            this.userId = userId;
            this.userName = userName;
            this.game = game;
        }

        @Override
        public String toString() {
            return "User{" +
                    "userId='" + userId + '\'' +
                    ", userName='" + userName + '\'' +
                    ", game=" + game +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            User user = (User) o;
            return Objects.equals(userId, user.userId) && Objects.equals(userName, user.userName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(userId, userName);
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<User> input = getProgressPCollection(pipeline);

        Schema type = Schema.builder()
                .addStringField("userId")
                .addStringField("userName")
                .addInt32Field("score")
                .addStringField("gameId")
                .addStringField("date")
                .build();

        PCollection<String> pCollection = input
                .apply(MapElements.into(TypeDescriptor.of(Object.class)).via(it -> it))
                .setSchema(type,
                        TypeDescriptor.of(Object.class), row ->
                        {
                            User user = (User) row;
                            return Row.withSchema(type)
                                    .addValues(user.userId, user.userName, user.game.score, user.game.gameId, user.game.date)
                                    .build();
                        },
                        row -> new User(row.getString(0), row.getString(1),
                                new Game(row.getString(0), row.getInt32(2), row.getString(3), row.getString(4)))
                )
                .apply(Group.byFieldNames("userId").aggregateField("score", Sum.ofIntegers(), "total"))
                .apply(MapElements.into(TypeDescriptor.of(String.class)).via(row -> row.getRow(0).getValue(0)+" : "+row.getRow(1).getValue(0)));

        pCollection
                .apply("User flatten row", ParDo.of(new LogOutput<>("Flattened")));

        pipeline.run();
    }

    public static PCollection<User> getProgressPCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/game/small/gaming_data.csv"));
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);
        return rides.apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new ExtractUserProgressFn()));
    }

    static class ExtractUserProgressFn extends DoFn<String, User> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            c.output(new User(items[0], items[1], new Game(items[0], Integer.valueOf(items[2]), items[3], items[4])
            ));
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
