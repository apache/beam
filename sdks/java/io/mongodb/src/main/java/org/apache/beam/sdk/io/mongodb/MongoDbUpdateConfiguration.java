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
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Configuration class representing MongoDB Update/Upsert settings. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class MongoDbUpdateConfiguration implements Serializable {

  public abstract @Nullable String getFindKey();

  public abstract @Nullable String getUpdateKey();

  public abstract @Nullable Boolean getIsUpsert();

  public abstract @Nullable List<MongoDbUpdateField> getUpdateFields();

  public static Builder builder() {
    return new AutoValue_MongoDbUpdateConfiguration.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFindKey(@Nullable String findKey);

    public abstract Builder setUpdateKey(@Nullable String updateKey);

    public abstract Builder setIsUpsert(@Nullable Boolean isUpsert);

    public abstract Builder setUpdateFields(@Nullable List<MongoDbUpdateField> updateFields);

    public abstract MongoDbUpdateConfiguration build();
  }
}
