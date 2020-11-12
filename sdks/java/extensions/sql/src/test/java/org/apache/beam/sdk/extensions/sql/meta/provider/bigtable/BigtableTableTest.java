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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigtable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.BINARY_COLUMN;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.BOOL_COLUMN;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.DOUBLE_COLUMN;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.FAMILY_TEST;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.KEY1;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.KEY2;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.LATER;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.LONG_COLUMN;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.NOW;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.STRING_COLUMN;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.booleanToByteArray;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.doubleToByteArray;

import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import org.apache.beam.sdk.io.gcp.testing.BigtableEmulatorWrapper;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class BigtableTableTest {

  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR = BigtableEmulatorRule.create();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private static BigtableEmulatorWrapper emulatorWrapper;

  @BeforeClass
  public static void setUp() throws Exception {
    emulatorWrapper =
        new BigtableEmulatorWrapper(BIGTABLE_EMULATOR.getPort(), "fakeProject", "fakeInstance");
    setupTable();
  }

  protected static String getLocation() {
    return String.format(
        "localhost:%s/bigtable/projects/fakeProject/instances/fakeInstance/tables/beamTable",
        BIGTABLE_EMULATOR.getPort());
  }

  private static void setupTable() throws Exception {
    emulatorWrapper.createTable("beamTable", FAMILY_TEST);
    writeRow(KEY1);
    writeRow(KEY2);
  }

  private static void writeRow(String key) throws Exception {
    emulatorWrapper.writeRow(
        key, "beamTable", FAMILY_TEST, BOOL_COLUMN, booleanToByteArray(true), NOW);
    emulatorWrapper.writeRow(
        key, "beamTable", FAMILY_TEST, BOOL_COLUMN, booleanToByteArray(false), LATER);
    emulatorWrapper.writeRow(
        key, "beamTable", FAMILY_TEST, STRING_COLUMN, "string1".getBytes(UTF_8), NOW);
    emulatorWrapper.writeRow(
        key, "beamTable", FAMILY_TEST, STRING_COLUMN, "string2".getBytes(UTF_8), LATER);
    emulatorWrapper.writeRow(
        key, "beamTable", FAMILY_TEST, LONG_COLUMN, Longs.toByteArray(1L), NOW);
    emulatorWrapper.writeRow(
        key, "beamTable", FAMILY_TEST, LONG_COLUMN, Longs.toByteArray(2L), LATER);
    emulatorWrapper.writeRow(
        key, "beamTable", FAMILY_TEST, DOUBLE_COLUMN, doubleToByteArray(1.10), NOW);
    emulatorWrapper.writeRow(
        key, "beamTable", FAMILY_TEST, DOUBLE_COLUMN, doubleToByteArray(2.20), LATER);
    emulatorWrapper.writeRow(
        key, "beamTable", FAMILY_TEST, BINARY_COLUMN, "blob1".getBytes(UTF_8), LATER);
    emulatorWrapper.writeRow(
        key, "beamTable", FAMILY_TEST, BINARY_COLUMN, "blob2".getBytes(UTF_8), LATER);
  }
}
