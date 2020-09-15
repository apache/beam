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
package org.apache.beam.sdk.io.mongodb;

import com.google.auto.value.AutoValue;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.lang.Nullable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.bson.BsonDocument;
import org.bson.Document;

/** Builds a MongoDB AggregateIterable object. */
@Experimental(Kind.SOURCE_SINK)
@AutoValue
public abstract class AggregationQuery
    implements SerializableFunction<MongoCollection<Document>, MongoCursor<Document>> {

  abstract List<BsonDocument> mongoDbPipeline();

  abstract @Nullable BsonDocument bucket();

  private static Builder builder() {
    return new AutoValue_AggregationQuery.Builder().setMongoDbPipeline(new ArrayList<>());
  }

  abstract Builder toBuilder();

  public static AggregationQuery create() {
    return builder().build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setMongoDbPipeline(List<BsonDocument> mongoDbPipeline);

    abstract Builder setBucket(BsonDocument bucket);

    abstract AggregationQuery build();
  }

  public AggregationQuery withMongoDbPipeline(List<BsonDocument> mongoDbPipeline) {
    return toBuilder().setMongoDbPipeline(mongoDbPipeline).build();
  }

  @Override
  public MongoCursor<Document> apply(MongoCollection<Document> collection) {
    if (bucket() != null) {
      if (mongoDbPipeline().size() == 1) {
        mongoDbPipeline().add(bucket());
      } else {
        mongoDbPipeline().set(mongoDbPipeline().size() - 1, bucket());
      }
    }
    return collection.aggregate(mongoDbPipeline()).iterator();
  }
}
