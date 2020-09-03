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

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

import java.io.IOException;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of {@link DefaultFilenamePolicy}. */
@RunWith(JUnit4.class)
public class DefaultFilenamePolicyTest {

  @Before
  public void setup() {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10738
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
  }

  private static String constructName(
      String baseFilename,
      String shardTemplate,
      String suffix,
      int shardNum,
      int numShards,
      String paneStr,
      String windowStr) {
    ResourceId constructed =
        DefaultFilenamePolicy.constructName(
            FileSystems.matchNewResource(baseFilename, false),
            shardTemplate,
            suffix,
            shardNum,
            numShards,
            paneStr,
            windowStr);
    return constructed.toString();
  }

  @Test
  public void testConstructName() {
    assertEquals(
        "/path/to/output-001-of-123.txt",
        constructName("/path/to/output", "-SSS-of-NNN", ".txt", 1, 123, null, null));

    assertEquals(
        "/path/to/out.txt/part-00042",
        constructName("/path/to/out.txt", "/part-SSSSS", "", 42, 100, null, null));

    assertEquals("/path/to/out.txt", constructName("/path/to/ou", "t.t", "xt", 1, 1, null, null));

    assertEquals(
        "/path/to/out0102shard.txt",
        constructName("/path/to/out", "SSNNshard", ".txt", 1, 2, null, null));

    assertEquals(
        "/path/to/out-2/1.part-1-of-2.txt",
        constructName("/path/to/out", "-N/S.part-S-of-N", ".txt", 1, 2, null, null));
  }

  @Test
  public void testConstructNameWithLargeShardCount() {
    assertEquals(
        "/out-100-of-5000.txt", constructName("/out", "-SS-of-NN", ".txt", 100, 5000, null, null));
  }

  @Test
  public void testConstructWindowedName() {
    assertEquals(
        "/path/to/output-001-of-123.txt",
        constructName("/path/to/output", "-SSS-of-NNN", ".txt", 1, 123, null, null));

    assertEquals(
        "/path/to/output-001-of-123-PPP-W.txt",
        constructName("/path/to/output", "-SSS-of-NNN-PPP-W", ".txt", 1, 123, null, null));

    assertEquals(
        "/path/to/out" + ".txt/part-00042-myPaneStr-myWindowStr",
        constructName(
            "/path/to/out.txt", "/part-SSSSS-P-W", "", 42, 100, "myPaneStr", "myWindowStr"));

    assertEquals(
        "/path/to/out.txt",
        constructName("/path/to/ou", "t.t", "xt", 1, 1, "myPaneStr2", "anotherWindowStr"));

    assertEquals(
        "/path/to/out0102shard-oneMoreWindowStr-anotherPaneStr.txt",
        constructName(
            "/path/to/out", "SSNNshard-W-P", ".txt", 1, 2, "anotherPaneStr", "oneMoreWindowStr"));

    assertEquals(
        "/out-2/1.part-1-of-2-slidingWindow1-myPaneStr3-windowslidingWindow1-"
            + "panemyPaneStr3.txt",
        constructName(
            "/out",
            "-N/S.part-S-of-N-W-P-windowW-paneP",
            ".txt",
            1,
            2,
            "myPaneStr3",
            "slidingWindow1"));

    // test first/last pane
    assertEquals(
        "/out.txt/part-00042-myWindowStr-pane-11-true-false",
        constructName(
            "/out.txt", "/part-SSSSS-W-P", "", 42, 100, "pane-11-true-false", "myWindowStr"));

    assertEquals(
        "/path/to/out.txt",
        constructName("/path/to/ou", "t.t", "xt", 1, 1, "pane", "anotherWindowStr"));

    assertEquals(
        "/out0102shard-oneMoreWindowStr-pane--1-false-false-pane--1-false-false.txt",
        constructName(
            "/out", "SSNNshard-W-P-P", ".txt", 1, 2, "pane--1-false-false", "oneMoreWindowStr"));

    assertEquals(
        "/path/to/out-2/1.part-1-of-2-sWindow1-winsWindow1-ppaneL.txt",
        constructName(
            "/path/to/out", "-N/S.part-S-of-N-W-winW-pP", ".txt", 1, 2, "paneL", "sWindow1"));
  }

  @Test
  public void testParamsCoder() throws IOException {
    assertEquals(
        new DefaultFilenamePolicy.Params()
            .withBaseFilename(FileSystems.matchNewResource("/tmp/file1", false)),
        CoderUtils.clone(
            DefaultFilenamePolicy.ParamsCoder.of(),
            new DefaultFilenamePolicy.Params()
                .withBaseFilename(FileSystems.matchNewResource("/tmp/file1", false))));
    assertEquals(
        new DefaultFilenamePolicy.Params()
            .withBaseFilename(FileSystems.matchNewResource("/tmp/file2", false))
            .withSuffix(".json"),
        CoderUtils.clone(
            DefaultFilenamePolicy.ParamsCoder.of(),
            new DefaultFilenamePolicy.Params()
                .withBaseFilename(FileSystems.matchNewResource("/tmp/file2", false))
                .withSuffix(".json")));

    assertEquals(
        new DefaultFilenamePolicy.Params()
            .withBaseFilename(FileSystems.matchNewResource("/tmp/dir1/", true)),
        CoderUtils.clone(
            DefaultFilenamePolicy.ParamsCoder.of(),
            new DefaultFilenamePolicy.Params()
                .withBaseFilename(FileSystems.matchNewResource("/tmp/dir1/", true))));
    assertEquals(
        new DefaultFilenamePolicy.Params()
            .withBaseFilename(FileSystems.matchNewResource("/tmp/dir2/", true))
            .withSuffix(".json"),
        CoderUtils.clone(
            DefaultFilenamePolicy.ParamsCoder.of(),
            new DefaultFilenamePolicy.Params()
                .withBaseFilename(FileSystems.matchNewResource("/tmp/dir2/", true))
                .withSuffix(".json")));

    assertEquals(
        new DefaultFilenamePolicy.Params()
            .withBaseFilename(FileSystems.matchNewResource("/tmp/dir3", true)),
        CoderUtils.clone(
            DefaultFilenamePolicy.ParamsCoder.of(),
            new DefaultFilenamePolicy.Params()
                .withBaseFilename(FileSystems.matchNewResource("/tmp/dir3", true))));
    assertEquals(
        new DefaultFilenamePolicy.Params()
            .withBaseFilename(FileSystems.matchNewResource("/tmp/dir4", true))
            .withSuffix(".json"),
        CoderUtils.clone(
            DefaultFilenamePolicy.ParamsCoder.of(),
            new DefaultFilenamePolicy.Params()
                .withBaseFilename(FileSystems.matchNewResource("/tmp/dir4", true))
                .withSuffix(".json")));
  }
}
