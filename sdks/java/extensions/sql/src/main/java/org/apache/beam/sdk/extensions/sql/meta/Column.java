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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema.FieldType;

/**
 * Metadata class for a {@code BeamSqlTable} column.
 */
@AutoValue
public abstract class Column implements Serializable {
  // TODO: Add Nullable types.
  public abstract String getName();
  public abstract FieldType getFieldType();

  @Nullable
  public abstract String getComment();
  public abstract boolean isPrimaryKey();

  public static Builder builder() {
    return new org.apache.beam.sdk.extensions.sql.meta.AutoValue_Column.Builder();
  }

  /**
   * Builder class for {@link Column}.
   */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);
    public abstract Builder fieldType(FieldType fieldType);
    public abstract Builder comment(String comment);
    public abstract Builder primaryKey(boolean isPrimaryKey);
    public abstract Column build();
  }
}
