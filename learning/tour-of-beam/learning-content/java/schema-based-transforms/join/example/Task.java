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
//   name: join
//   description: Join example.
//   multifile: false
//   context_line: 46
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Task {
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    @DefaultSchema(JavaFieldSchema.class)
    public static class User {
        public Long userId;
        public String userName;
        public String userSurname;

        @SchemaCreate
        public User(Long userId, String userName, String userSurname) {
            this.userName = userName;
            this.userSurname = userSurname;
            this.userId = userId;
        }
    }

    // Location schema
    @DefaultSchema(JavaFieldSchema.class)
    public static class Location {
        public Long userId;
        public double latitude;
        public double longtitude;

        @SchemaCreate
        public Location(Long userId, double latitude, double longtitude) {
            this.userId = userId;
            this.latitude = latitude;
            this.longtitude = longtitude;
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        User user1 = new User(1L, "Andy", "Gross");
        User user2 = new User(2L, "May", "Kim");
        Location location = new Location(1L, 223.2, 16.8);

        PCollection<Object> userPCollection = pipeline.apply(Create.of(user1, user2));
        PCollection<Object> locationPCollection = pipeline.apply(Create.of(location));

        PCollection<Row> rowPCollection = userPCollection.apply(Join.innerJoin(locationPCollection).using("userId"));

        rowPCollection.apply(Select.fieldNames("lhs.userName", "lhs.userSurname", "rhs.latitude", "rhs.longtitude"))
                .apply("User location", ParDo.of(new LogOutput<>("User with location")));

        pipeline.run();

    }

    static class LogOutput<T> extends DoFn<T, T> {

        private String prefix;

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
