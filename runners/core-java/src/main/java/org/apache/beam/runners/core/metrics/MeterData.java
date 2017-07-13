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
import org.apache.beam.sdk.metrics.MeterResult;
import org.apache.beam.sdk.metrics.MetricData;

/**
 * The snapshot data of a meter.
 */
@AutoValue
public abstract class MeterData implements MetricData<MeterData> {

  public abstract long count();
  public abstract double m1();
  public abstract double m5();
  public abstract double m15();
  public abstract double mean();

  public static MeterData create(long count, double m1, double m5, double m15, double mean) {
    return new AutoValue_MeterData(count, m1, m5, m15, mean);
  }

  @Override
  public MeterData merge(MeterData other) {
    return create(count() + other.count(),
        m1() + other.m1(),
        m5() + other.m5(),
        m15() + other.m15(),
        mean() + other.mean());
  }

  public MeterData combine(MeterData other) {
    return merge(other);
  }

  public static MeterData zero() {
    return create(0, 0, 0, 0, 0);
  }

  public MeterResult extractResult(){
    return MeterResult.create(count(), m1(), m5(), m15(), mean());
  }
}
