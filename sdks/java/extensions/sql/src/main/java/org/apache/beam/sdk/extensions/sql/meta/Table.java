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

package org.apache.beam.sdk.extensions.sql.meta;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;

/**
 * Represents the metadata of a {@code BeamSqlTable}.
 */
@AutoValue
public abstract class Table implements Serializable {
  /** type of the table. */
  public abstract String getType();
  public abstract String getName();
  public abstract Schema getSchema();
  @Nullable
  public abstract String getComment();
  @Nullable
  public abstract String getLocation();
  @Nullable
  public abstract JSONObject getProperties();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new org.apache.beam.sdk.extensions.sql.meta.AutoValue_Table.Builder();
  }

  /**
   * Builder class for {@link Table}.
   */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder type(String type);
    public abstract Builder name(String name);
    public abstract Builder schema(Schema getSchema);
    public abstract Builder comment(String name);
    public abstract Builder location(String location);
    public abstract Builder properties(JSONObject properties);
    public abstract Table build();
  }
}
