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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link TableDestinationCoderV4}. */
@RunWith(JUnit4.class)
public class TableDestinationCoderV4Test {

  private static final Coder<TableDestination> TEST_CODER = TableDestinationCoderV4.of();

  @Test
  public void testDecodeEncodeEqualWithLabels() throws Exception {
    TableDestination value =
        new TableDestination(
                "project:dataset.table",
                "description",
                new TimePartitioning().setType("DAY").setField("date"),
                new Clustering().setFields(ImmutableList.of("date")))
            .withLabels(ImmutableMap.of("env", "prod", "team", "data"));

    CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
  }

  @Test
  public void testDecodeEncodeEqualWithoutLabels() throws Exception {
    CoderProperties.coderDecodeEncodeEqual(
        TEST_CODER, new TableDestination("project:dataset.table", null));
  }

  @Test
  public void testLabelInsertionOrderDoesNotChangeEncoding() throws Exception {
    Map<String, String> envFirst = new LinkedHashMap<>();
    envFirst.put("env", "prod");
    envFirst.put("team", "data");
    Map<String, String> teamFirst = new LinkedHashMap<>();
    teamFirst.put("team", "data");
    teamFirst.put("env", "prod");

    CoderProperties.coderDeterministic(
        TEST_CODER,
        new TableDestination("project:dataset.table", null).withLabels(envFirst),
        new TableDestination("project:dataset.table", null).withLabels(teamFirst));
  }
}
