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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

@AutoValue
public abstract class UpdateField implements Serializable {

  @Pure
  abstract @Nullable String updateOperator();

  @Pure
  abstract @Nullable String sourceField();

  @Pure
  abstract @Nullable String destField();

  private static Builder builder() {
    return new AutoValue_UpdateField.Builder().setSourceField(null);
  }

  abstract UpdateField.Builder toBuilder();

  private static UpdateField create() {
    return builder().build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract UpdateField.Builder setUpdateOperator(@Nullable String updateOperator);

    abstract UpdateField.Builder setSourceField(@Nullable String sourceField);

    abstract UpdateField.Builder setDestField(@Nullable String destField);

    abstract UpdateField build();
  }

  /** Sets the limit of documents to find. */
  public static UpdateField fullUpdate(String updateOperator, String destField) {
    return create().toBuilder().setUpdateOperator(updateOperator).setDestField(destField).build();
  }

  public static UpdateField fieldUpdate(
      String updateOperator, String sourceField, String destField) {
    return create()
        .toBuilder()
        .setUpdateOperator(updateOperator)
        .setSourceField(sourceField)
        .setDestField(destField)
        .build();
  }
}
