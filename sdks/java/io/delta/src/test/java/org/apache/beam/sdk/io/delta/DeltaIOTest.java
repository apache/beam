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
package org.apache.beam.sdk.io.delta;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.delta.DeltaIO.ReadRows;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for the {@link DeltaIO}. */
@RunWith(JUnit4.class)
public class DeltaIOTest {

  @Test
  public void testReadRowsBuilderAndGetters() {
    String tablePath = "/path/to/table";
    long version = 5L;
    String timestamp = "2026-05-20T15:43:26Z";
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.defaultFS", "file:///");

    ReadRows readRows =
        DeltaIO.readRows()
            .from(tablePath)
            .withVersion(version)
            .withTimestamp(timestamp)
            .withConfig(hadoopConfig);

    Assert.assertEquals(tablePath, readRows.getTablePath());
    Assert.assertEquals(Long.valueOf(version), readRows.getVersion());
    Assert.assertEquals(timestamp, readRows.getTimestamp());
    Assert.assertEquals(hadoopConfig, readRows.getHadoopConfig());
  }

  @Test
  public void testReadRowsNullDefaults() {
    ReadRows readRows = DeltaIO.readRows();

    Assert.assertNull(readRows.getTablePath());
    Assert.assertNull(readRows.getVersion());
    Assert.assertNull(readRows.getTimestamp());
    Assert.assertNull(readRows.getHadoopConfig());
  }
}
