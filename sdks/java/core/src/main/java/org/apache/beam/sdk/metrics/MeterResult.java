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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * The result of a {@link Meter} metric.
 */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class MeterResult {

  public abstract long count();
  public abstract double m1();
  public abstract double m5();
  public abstract double m15();
  public abstract double mean();

  public static final MeterResult ZERO = create(0, 0, 0, 0, 0);

  public static MeterResult create(long count, double m1, double m5, double m15, double mean) {
    return new AutoValue_MeterResult(count, m1, m5, m15, mean);
  }
}
