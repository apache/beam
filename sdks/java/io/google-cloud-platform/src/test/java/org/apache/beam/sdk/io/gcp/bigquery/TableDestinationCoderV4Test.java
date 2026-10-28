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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.RangePartitioning;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.util.Arrays;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link TableDestinationCoderV4}. */
@RunWith(JUnit4.class)
public class TableDestinationCoderV4Test {

  private static final Coder<TableDestination> TEST_CODER = TableDestinationCoderV4.of();

  private static final RangePartitioning RANGE_PARTITIONING =
      new RangePartitioning()
          .setField("id")
          .setRange(new RangePartitioning.Range().setStart(0L).setEnd(100L).setInterval(10L));

  @Test
  public void testDecodeEncodeEqualWithRangePartitioning() throws Exception {
    TableDestination value =
        new TableDestination(
            "project:dataset.table",
            "description",
            null,
            new Clustering().setFields(Arrays.asList("a", "b")),
            RANGE_PARTITIONING);

    CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);

    TableDestination decoded =
        CoderUtils.decodeFromByteArray(TEST_CODER, CoderUtils.encodeToByteArray(TEST_CODER, value));
    assertEquals(RANGE_PARTITIONING, decoded.getRangePartitioning());
    assertEquals(value.getJsonClustering(), decoded.getJsonClustering());
    assertNull(decoded.getJsonTimePartitioning());
  }

  @Test
  public void testDecodeEncodeEqualWithoutRangePartitioning() throws Exception {
    TableDestination value =
        new TableDestination(
            "project:dataset.table",
            null,
            new TimePartitioning().setType("DAY").setField("date"),
            new Clustering().setFields(Arrays.asList("a")));

    CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);

    TableDestination decoded =
        CoderUtils.decodeFromByteArray(TEST_CODER, CoderUtils.encodeToByteArray(TEST_CODER, value));
    assertEquals(value.getJsonTimePartitioning(), decoded.getJsonTimePartitioning());
    assertEquals(value.getJsonClustering(), decoded.getJsonClustering());
    assertNull(decoded.getJsonRangePartitioning());
  }

  @Test
  public void testEncodingExtendsV3() throws Exception {
    TableDestination value =
        new TableDestination(
            "project:dataset.table",
            "description",
            null,
            new Clustering().setFields(Arrays.asList("a")),
            RANGE_PARTITIONING);

    byte[] v3Bytes = CoderUtils.encodeToByteArray(TableDestinationCoderV3.of(), value);
    byte[] v4Bytes = CoderUtils.encodeToByteArray(TEST_CODER, value);

    // V4 writes the V3 encoding followed by the range partitioning.
    assertArrayEquals(v3Bytes, Arrays.copyOf(v4Bytes, v3Bytes.length));
  }

  @Test
  public void testCoderIsDeterministic() throws Exception {
    CoderProperties.coderDeterministic(
        TEST_CODER,
        new TableDestination("project:dataset.table", null, null, null, RANGE_PARTITIONING),
        new TableDestination("project:dataset.table", null, null, null, RANGE_PARTITIONING));
  }
}
