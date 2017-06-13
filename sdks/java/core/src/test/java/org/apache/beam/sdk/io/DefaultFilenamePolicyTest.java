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
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.io.DefaultFilenamePolicy.constructName;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests of {@link DefaultFilenamePolicy}.
 */
@RunWith(JUnit4.class)
public class DefaultFilenamePolicyTest {

  @Test
  public void testConstructName() {
    assertEquals(FileSystems.matchNewResource("path/to/output-001-of-123.txt", false),
        constructName(FileSystems.matchNewResource("path/to/output", false),
            "-SSS-of-NNN", ".txt", 1, 123, null, null));

    assertEquals(FileSystems.matchNewResource("path/to/out.txt/part-00042", false),
        constructName(FileSystems.matchNewResource("path/to/out.txt", false),
            "/part-SSSSS", "", 42, 100, null, null));

    assertEquals(FileSystems.matchNewResource("path/to/out.txt", false),
        constructName(FileSystems.matchNewResource("path/to/ou", false),
            "t.t", "xt", 1, 1, null, null));

    assertEquals(FileSystems.matchNewResource("path/to/out0102shard.txt", false),
        constructName(FileSystems.matchNewResource("path/to/out", false),
            "SSNNshard", ".txt", 1, 2, null, null));

    assertEquals(FileSystems.matchNewResource("path/to/out-2/1.part-1-of-2.txt", false),
        constructName(FileSystems.matchNewResource("path/to/out", false),
            "-N/S.part-S-of-N", ".txt", 1, 2, null, null));
  }

  @Test
  public void testConstructNameWithLargeShardCount() {
    assertEquals(FileSystems.matchNewResource("out-100-of-5000.txt", false),
        constructName(FileSystems.matchNewResource("out", false),
            "-SS-of-NN", ".txt", 100, 5000, null, null));
  }

  @Test
  public void testConstructWindowedName() {
    assertEquals(FileSystems.matchNewResource("path/to/output-001-of-123.txt", false),
        constructName(FileSystems.matchNewResource("path/to/output", false),
            "-SSS-of-NNN", ".txt", 1, 123, null, null));

    assertEquals(FileSystems.matchNewResource("path/to/output-001-of-123-PPP-W.txt", false),
        constructName(FileSystems.matchNewResource("path/to/output", false),
            "-SSS-of-NNN-PPP-W", ".txt", 1, 123, null, null));

    assertEquals(FileSystems.matchNewResource("path/to/out"
            + ".txt/part-00042-myPaneStr-myWindowStr", false),
        constructName(FileSystems.matchNewResource("path/to/out.txt", false),
            "/part-SSSSS-P-W", "", 42, 100, "myPaneStr",
            "myWindowStr"));

    assertEquals(FileSystems.matchNewResource("path/to/out.txt", false),
        constructName(FileSystems.matchNewResource("path/to/ou", false),
            "t.t", "xt", 1, 1, "myPaneStr2",
        "anotherWindowStr"));

    assertEquals(FileSystems.matchNewResource(
        "path/to/out0102shard-oneMoreWindowStr-anotherPaneStr.txt", false),
        constructName(FileSystems.matchNewResource("path/to/out", false),
            "SSNNshard-W-P", ".txt", 1, 2, "anotherPaneStr",
            "oneMoreWindowStr"));

    assertEquals(FileSystems.matchNewResource(
        "out-2/1.part-1-of-2-slidingWindow1-myPaneStr3-windowslidingWindow1-"
        + "panemyPaneStr3.txt", false),
        constructName(FileSystems.matchNewResource("out", false),
            "-N/S.part-S-of-N-W-P-windowW-paneP", ".txt", 1, 2, "myPaneStr3",
        "slidingWindow1"));

    // test first/last pane
    assertEquals(FileSystems.matchNewResource("out"
            + ".txt/part-00042-myWindowStr-pane-11-true-false", false),
        constructName(FileSystems.matchNewResource("out.txt", false),
            "/part-SSSSS-W-P", "", 42, 100, "pane-11-true-false",
            "myWindowStr"));

    assertEquals(FileSystems.matchNewResource("path/to/out.txt", false),
        constructName(FileSystems.matchNewResource("path/to/ou", false),
            "t.t", "xt", 1, 1, "pane",
        "anotherWindowStr"));

    assertEquals(FileSystems.matchNewResource(
        "out0102shard-oneMoreWindowStr-pane--1-false-false-pane--1-false-false.txt", false),
        constructName(FileSystems.matchNewResource("out", false),
            "SSNNshard-W-P-P", ".txt", 1, 2, "pane--1-false-false",
            "oneMoreWindowStr"));

    assertEquals(FileSystems.matchNewResource(
        "path/to/out-2/1.part-1-of-2-sWindow1-winsWindow1-ppaneL.txt", false),
        constructName(FileSystems.matchNewResource("path/to/out", false),
        "-N/S.part-S-of-N-W-winW-pP", ".txt", 1, 2, "paneL", "sWindow1"));
  }

}
