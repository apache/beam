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

/**
 * A no-op implementation of Counter. This class exists to provide a default if an implementation of
 * MetricsContainer does not override a Counter getter.
 */
public class NoOpCounter implements Counter {

  private static final NoOpCounter singleton = new NoOpCounter();
  private static final MetricName name = MetricName.named(NoOpCounter.class, "singleton");

  private NoOpCounter() {}

  @Override
  public void inc() {}

  @Override
  public void inc(long n) {}

  @Override
  public void dec() {}

  @Override
  public void dec(long n) {}

  @Override
  public MetricName getName() {
    return name;
  }

  public static NoOpCounter getInstance() {
    return singleton;
  }
}
