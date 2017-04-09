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
package org.apache.beam.sdk.metrics;

import com.google.auto.value.AutoValue;

/**
 Data describing a {@link Counter} metric.
 */
@AutoValue
public abstract class CounterData implements MetricData<Long> {

  public static final CounterData EMPTY = create(0);

  public abstract long value();

  public static CounterData create(long value) {
    return new AutoValue_CounterData(value);
  }

  @Override
  public CounterData combine(MetricData<Long> other) {
    return create(value() + ((CounterData) other).value());
  }

  @Override
  public Long extractResult() {
    return value();
  }
}
