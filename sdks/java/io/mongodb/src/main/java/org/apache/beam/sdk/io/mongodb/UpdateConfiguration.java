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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Builds a MongoDB UpdateConfiguration object. */
@Experimental(Kind.SOURCE_SINK)
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class UpdateConfiguration implements Serializable {

  abstract @Nullable String updateKey();

  abstract @Nullable List<UpdateField> updateFields();

  abstract boolean isUpsert();

  private static Builder builder() {
    return new AutoValue_UpdateConfiguration.Builder()
        .setUpdateFields(Collections.emptyList())
        .setIsUpsert(false);
  }

  abstract Builder toBuilder();

  public static UpdateConfiguration create() {
    return builder().build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setUpdateFields(@Nullable List<UpdateField> updateFields);

    abstract Builder setUpdateKey(@Nullable String updateKey);

    abstract Builder setIsUpsert(boolean isUpsert);

    abstract UpdateConfiguration build();
  }

  /**
   * Sets the configurations for multiple updates. Takes update operator, source field name and dest
   * field name for each one
   */
  public UpdateConfiguration withUpdateFields(UpdateField... updateFields) {
    return toBuilder().setUpdateFields(Arrays.asList(updateFields)).build();
  }

  /** Sets the filters to find. */
  public UpdateConfiguration withUpdateKey(String updateKey) {
    return toBuilder().setUpdateKey(updateKey).build();
  }

  public UpdateConfiguration withIsUpsert(boolean isUpsert) {
    return toBuilder().setIsUpsert(isUpsert).build();
  }
}
