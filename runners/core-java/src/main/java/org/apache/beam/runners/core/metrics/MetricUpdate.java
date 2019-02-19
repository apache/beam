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
package org.apache.beam.runners.core.metrics;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.metrics.MetricKey;

/**
 * Representation of a single metric update.
 *
 * @param <T> The type of value representing the update.
 */
@AutoValue
public abstract class MetricUpdate<T> implements Serializable {

  /** The key being updated. */
  public abstract MetricKey getKey();
  /** The value of the update. */
  public abstract T getUpdate();

  public static <T> MetricUpdate<T> create(MetricKey key, T update) {
    return new AutoValue_MetricUpdate<T>(key, update);
  }
}
