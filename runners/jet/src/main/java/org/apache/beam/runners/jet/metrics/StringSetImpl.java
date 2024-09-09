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
package org.apache.beam.runners.jet.metrics;

import org.apache.beam.runners.core.metrics.StringSetData;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.StringSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/** Implementation of {@link StringSet}. */
public class StringSetImpl extends AbstractMetric<StringSetData> implements StringSet {

  private final StringSetData stringSetData = StringSetData.empty();

  public StringSetImpl(MetricName name) {
    super(name);
  }

  @Override
  StringSetData getValue() {
    return stringSetData;
  }

  @Override
  public void add(String value) {
    if (stringSetData.stringSet().contains(value)) {
      return;
    }
    stringSetData.combine(StringSetData.create(ImmutableSet.of(value)));
  }

  @Override
  public void add(String... values) {
    stringSetData.combine(StringSetData.create(ImmutableSet.copyOf(values)));
  }
}
