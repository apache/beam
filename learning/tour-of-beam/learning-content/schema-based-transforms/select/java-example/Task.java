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
//   name: select
//   description: Select example.
//   multifile: false
//   context_line: 126
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
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    @DefaultSchema(JavaFieldSchema.class)
    public static class Game {
        public String userId;
        public String score;
        public String gameId;
        public String date;

        @SchemaCreate
        public Game(String userId, String score, String gameId, String date) {
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

        public Game game;

        @SchemaCreate
        public User(String userId, String userName, Game game) {
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
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        Schema shortInfoSchema = Schema.builder()
                .addStringField("userId")
                .addStringField("gameId")
                .build();

        Schema gameSchema = Schema.builder()
                .addStringField("userId")
                .addStringField("score")
                .addStringField("gameId")
                .addStringField("date")
                .build();

        Schema dataSchema = Schema.builder()
                .addStringField("userId")
                .addStringField("userName")
                .addRowField("game", gameSchema)
                .build();


        PCollection<User> input = getProgressPCollection(pipeline)
                .setSchema(dataSchema,
                        TypeDescriptor.of(User.class), row ->
                        {
                            User user = row;
                            Game game = user.game;

                            Row gameRow = Row.withSchema(gameSchema)
                                    .addValues(game.userId, game.score, game.gameId, game.date)
                                    .build();

                            return Row.withSchema(dataSchema)
                                    .addValues(user.userId, user.userName, gameRow).build();
                        },
                        row -> {
                            String userId = row.getValue("userId");
                            String userName = row.getValue("userName");
                            Row game = row.getValue("game");

                            String gameId = game.getValue("gameId");
                            String gameScore = game.getValue("score");
                            String gameDate = game.getValue("date");
                            return new User(userId,userName,
                                    new Game(userId,gameScore,gameId,gameDate));
                        });

        // Select [userId] and [userName]
        PCollection<Row> shortInfo = input
                .apply(Select.<User>fieldNames("userId", "userName").withOutputSchema(shortInfoSchema))
                .apply("User short info", ParDo.of(new LogOutput<>("Short Info")));

        // Select user [game]
        PCollection<Row> game = input
                .apply(Select.fieldNames("game.*"))
                .apply("User game", ParDo.of(new LogOutput<>("Game")));

        // Flattened row, select all fields
        PCollection<Row> flattened = input
                .apply(Select.flattenedSchema())
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
            c.output(new User(items[0], items[1], new Game(items[0], items[2], items[3], items[4])));
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
