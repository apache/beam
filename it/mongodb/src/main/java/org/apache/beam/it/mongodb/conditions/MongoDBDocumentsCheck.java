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
package org.apache.beam.it.mongodb.conditions;

import com.google.auto.value.AutoValue;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.mongodb.MongoDBResourceManager;
import org.checkerframework.checker.nullness.qual.Nullable;

/** ConditionCheck to validate if MongoDB has received a certain amount of documents. */
@AutoValue
public abstract class MongoDBDocumentsCheck extends ConditionCheck {

  abstract MongoDBResourceManager resourceManager();

  abstract String collectionName();

  abstract Integer minDocuments();

  abstract @Nullable Integer maxDocuments();

  @Override
  public String getDescription() {
    if (maxDocuments() != null) {
      return String.format(
          "MongoDB check if collection %s has between %d and %d documents",
          collectionName(), minDocuments(), maxDocuments());
    }
    return String.format(
        "MongoDB check if collection %s has %d documents", collectionName(), minDocuments());
  }

  @Override
  @SuppressWarnings("unboxing.of.nullable")
  public CheckResult check() {
    long totalDocuments = resourceManager().countCollection(collectionName());
    if (totalDocuments < minDocuments()) {
      return new CheckResult(
          false, String.format("Expected %d but has only %d", minDocuments(), totalDocuments));
    }
    if (maxDocuments() != null && totalDocuments > maxDocuments()) {
      return new CheckResult(
          false,
          String.format(
              "Expected up to %d but found %d documents", maxDocuments(), totalDocuments));
    }

    if (maxDocuments() != null) {
      return new CheckResult(
          true,
          String.format(
              "Expected between %d and %d documents and found %d",
              minDocuments(), maxDocuments(), totalDocuments));
    }

    return new CheckResult(
        true,
        String.format(
            "Expected at least %d documents and found %d", minDocuments(), totalDocuments));
  }

  public static Builder builder(MongoDBResourceManager resourceManager, String collectionName) {
    return new AutoValue_MongoDBDocumentsCheck.Builder()
        .setResourceManager(resourceManager)
        .setCollectionName(collectionName);
  }

  /** Builder for {@link MongoDBDocumentsCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setResourceManager(MongoDBResourceManager resourceManager);

    public abstract Builder setCollectionName(String collectionName);

    public abstract Builder setMinDocuments(Integer minDocuments);

    public abstract Builder setMaxDocuments(Integer maxDocuments);

    abstract MongoDBDocumentsCheck autoBuild();

    public MongoDBDocumentsCheck build() {
      return autoBuild();
    }
  }
}
