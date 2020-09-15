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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Builds a MongoDB FindQuery object. */
@Experimental(Kind.SOURCE_SINK)
@AutoValue
public abstract class FindQuery
    implements SerializableFunction<MongoCollection<Document>, MongoCursor<Document>> {

  abstract @Nullable BsonDocument filters();

  abstract int limit();

  abstract List<String> projection();

  private static Builder builder() {
    return new AutoValue_FindQuery.Builder()
        .setLimit(0)
        .setProjection(Collections.emptyList())
        .setFilters(new BsonDocument());
  }

  abstract Builder toBuilder();

  public static FindQuery create() {
    return builder().build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setFilters(@Nullable BsonDocument filters);

    abstract Builder setLimit(int limit);

    abstract Builder setProjection(List<String> projection);

    abstract FindQuery build();
  }

  /** Sets the filters to find. */
  private FindQuery withFilters(BsonDocument filters) {
    return toBuilder().setFilters(filters).build();
  }

  /** Convert the Bson filters into a BsonDocument via default encoding. */
  static BsonDocument bson2BsonDocument(Bson filters) {
    return filters.toBsonDocument(BasicDBObject.class, MongoClient.getDefaultCodecRegistry());
  }

  /** Sets the filters to find. */
  public FindQuery withFilters(Bson filters) {
    return withFilters(bson2BsonDocument(filters));
  }

  /** Sets the limit of documents to find. */
  public FindQuery withLimit(int limit) {
    return toBuilder().setLimit(limit).build();
  }

  /** Sets the projection. */
  public FindQuery withProjection(List<String> projection) {
    checkArgument(projection != null, "projection can not be null");
    return toBuilder().setProjection(projection).build();
  }

  @Override
  public MongoCursor<Document> apply(MongoCollection<Document> collection) {
    return collection
        .find()
        .filter(filters())
        .limit(limit())
        .projection(Projections.include(projection()))
        .iterator();
  }
}
