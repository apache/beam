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

import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.extractUrn;
import static org.junit.Assert.assertThat;

import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfoSpecs;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MonitoringInfoConstants}. */
@RunWith(JUnit4.class)
public class MonitoringInfoConstantsTest {
  @Test
  public void testUniqueUrnsDefinedForAllSpecs() {
    Multimap<String, MonitoringInfoSpecs.Enum> urnToEnum = ArrayListMultimap.create();
    for (MonitoringInfoSpecs.Enum value : MonitoringInfoSpecs.Enum.values()) {
      if (value != MonitoringInfoSpecs.Enum.UNRECOGNIZED) {
        urnToEnum.put(extractUrn(value), value);
      }
    }
    for (String urn : ImmutableSet.copyOf(urnToEnum.keySet())) {
      if (urnToEnum.get(urn).size() == 1) {
        urnToEnum.removeAll(urn);
      }
    }
    assertThat(urnToEnum.entries(), Matchers.empty());
  }
}
