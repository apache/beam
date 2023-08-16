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
package org.apache.beam.sdk.testutils.publishing;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public final class InfluxDBPublisherTest {

  @Test
  public void testNexmarkDataPoints() {
    Map<String, Object> measurement =
        ImmutableMap.<String, Object>builder()
            .put("measurement", "name")
            .put("timestamp", 9999L)
            .put("runtimeMs", "1000i")
            .put("numResults", "10i")
            .build();
    List<Map<String, Object>> measurements =
        ImmutableList.of(measurement, measurement, measurement);

    Map<String, String> tags =
        ImmutableMap.of(
            "runner", "test",
            "tag", "value");

    String actual = InfluxDBPublisher.nexmarkDataPoints(measurements, tags);
    String expected = "name,runner=test,tag=value runtimeMs=1000i,numResults=10i 9999\n";

    assertEquals(expected + expected + expected, actual);
  }

  @Test
  public void testNamedTestResultToDataPoint() {
    NamedTestResult result = NamedTestResult.create("id1", "9999", "metric1", 100);

    String actual = result.toInfluxDBDataPoint("name").toString();
    String expected = "name,test_id=id1,metric=metric1 value=100.0";

    assertEquals(expected, actual);
  }

  @Test
  public void testDataPointToString() {
    Map<String, String> tags = ImmutableMap.of("tag1", "t1", "tag2", "t2");
    Map<String, Number> fields = ImmutableMap.of("integer", 100, "float", 100.0);

    assertEquals(
        "m1,tag1=t1,tag2=t2 integer=100i,float=100.0 999",
        InfluxDBPublisher.dataPoint("m1", tags, fields, 999L).toString());
    assertEquals(
        "m1,tag1=t1,tag2=t2 integer=100i,float=100.0",
        InfluxDBPublisher.dataPoint("m1", tags, fields, null).toString());
  }
}
