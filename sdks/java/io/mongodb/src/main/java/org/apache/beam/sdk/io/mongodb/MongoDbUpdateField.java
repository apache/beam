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
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Configuration class representing an individual MongoDB field update. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class MongoDbUpdateField implements Serializable {

  public abstract @Nullable String getUpdateOperator();

  public abstract @Nullable String getSourceField();

  public abstract @Nullable String getDestField();

  public static Builder builder() {
    return new AutoValue_MongoDbUpdateField.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setUpdateOperator(@Nullable String updateOperator);

    public abstract Builder setSourceField(@Nullable String sourceField);

    public abstract Builder setDestField(@Nullable String destField);

    public abstract MongoDbUpdateField build();
  }
}
