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

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for {@link Lineage}. */
@RunWith(JUnit4.class)
public class LineageTest {
  @Test
  public void testGetFqName() {
    Map<String, String> testCases =
        ImmutableMap.<String, String>builder()
            .put("apache-beam", "apache-beam")
            .put("`apache-beam`", "`apache-beam`")
            .put("apache.beam", "`apache.beam`")
            .put("apache:beam", "`apache:beam`")
            .put("apache beam", "`apache beam`")
            .put("`apache beam`", "`apache beam`")
            .put("apache\tbeam", "`apache\tbeam`")
            .put("apache\nbeam", "`apache\nbeam`")
            .build();
    testCases.forEach(
        (key, value) ->
            assertEquals("apache:" + value, Lineage.getFqName("apache", ImmutableList.of(key))));
    testCases.forEach(
        (key, value) ->
            assertEquals(
                "apache:beam:" + value,
                Lineage.getFqName("apache", "beam", ImmutableList.of(key))));
    testCases.forEach(
        (key, value) ->
            assertEquals(
                "apache:beam:" + value + "." + value,
                Lineage.getFqName("apache", "beam", ImmutableList.of(key, key))));
  }

  @Test
  public void testEnableDisableLineage() {
    StringSet mockMetric = Mockito.mock(StringSet.class);
    Lineage lineage = new Lineage(mockMetric);

    try {
      Lineage.resetDefaultPipelineOptions();
      lineage.add("beam:path");
      Mockito.verify(mockMetric, Mockito.times(1)).add("beam:path");

      Lineage.setDefaultPipelineOptions(
          PipelineOptionsFactory.fromArgs("--experiments=disable_lineage").create());
      lineage.add("beam:path2");
      Mockito.verify(mockMetric, Mockito.never()).add("beam:path2");
    } finally {
      Lineage.resetDefaultPipelineOptions();
    }
  }
}
