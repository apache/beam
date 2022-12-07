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
//   name: co-group
//   description: Co-group example.
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
import org.apache.beam.sdk.schemas.transforms.CoGroup;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Task {
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    // User schema
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

    // UserPurchase schema
    @DefaultSchema(JavaFieldSchema.class)
    public static class UserPurchase {
        public Long userId;
        public long cost;
        public double transactionDuration;

        @SchemaCreate
        public UserPurchase(Long userId, long cost, double transactionDuration) {
            this.userId = userId;
            this.cost = cost;
            this.transactionDuration = transactionDuration;
        }
    }

    // Location schema
    @DefaultSchema(JavaFieldSchema.class)
    public static class Location {
        public Long userId;
        public String countryName;

        @SchemaCreate
        public Location(Long userId, String countryName) {
            this.userId = userId;
            this.countryName = countryName;
        }
    }


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        Location location1 = new Location(1L, "America");
        Location location2 = new Location(2L, "Brazilian");
        Location location3 = new Location(3L, "Mexico");

        PCollection<Object> locationPCollection = pipeline.apply(Create.of(location1, location2, location3));

        UserPurchase userPurchase1 = new UserPurchase(1L, 123, 22);
        UserPurchase userPurchase2 = new UserPurchase(2L, 645, 86);
        UserPurchase userPurchase3 = new UserPurchase(3L, 741, 33);

        PCollection<Object> userPurchasePCollection = pipeline.apply(Create.of(userPurchase1, userPurchase2, userPurchase3));

        User user1 = new User(1L, "Andy", "Mira");
        User user2 = new User(2L, "Tom", "Larry");
        User user3 = new User(3L, "Kerry", "Jim");

        PCollection<Object> userPCollection = pipeline.apply(Create.of(user1, user2, user3));


        PCollection<Row> coGroupPCollection =
                PCollectionTuple.of("userPurchase", userPurchasePCollection, "user", userPCollection, "location", locationPCollection)
                        .apply(CoGroup.join(CoGroup.By.fieldNames("userId")));


        coGroupPCollection.apply(Select.fieldNames("user.userName", "user.userSurname", "location.countryName", "userPurchase.cost", "userPurchase.transactionDuration"))
                .apply("User Purchase", ParDo.of(new LogOutput<>("CoGroup")));
        pipeline.run();
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
