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

package org.apache.beam.sdk.extensions.sql.impl.rule;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.Row;

/**
 * <b>For internal use only; no backwards compatibility guarantees.</b>
 *
 * <p>Represents a field with a window function call in a SQL expression.
 */
@Internal
@AutoValue
public abstract class AggregateWindowField {
  public abstract int fieldIndex();
  public abstract WindowFn<Row, ? extends BoundedWindow> windowFn();

  static Builder builder() {
    return new AutoValue_AggregateWindowField.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setFieldIndex(int fieldIndex);
    abstract Builder setWindowFn(WindowFn<Row, ? extends BoundedWindow> window);
    abstract AggregateWindowField build();
  }
}
