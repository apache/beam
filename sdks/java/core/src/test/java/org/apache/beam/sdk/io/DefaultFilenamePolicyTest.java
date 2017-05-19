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
import static org.apache.beam.sdk.io.DefaultFilenamePolicy.isWindowedTemplate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    assertEquals("output-001-of-123.txt",
        constructName("output", "-SSS-of-NNN", ".txt", 1, 123));

    assertEquals("out.txt/part-00042",
        constructName("out.txt", "/part-SSSSS", "", 42, 100));

    assertEquals("out.txt",
        constructName("ou", "t.t", "xt", 1, 1));

    assertEquals("out0102shard.txt",
        constructName("out", "SSNNshard", ".txt", 1, 2));

    assertEquals("out-2/1.part-1-of-2.txt",
        constructName("out", "-N/S.part-S-of-N", ".txt", 1, 2));
  }

  @Test
  public void testConstructNameWithLargeShardCount() {
    assertEquals("out-100-of-5000.txt",
        constructName("out", "-SS-of-NN", ".txt", 100, 5000));
  }

  @Test
  public void testIsWindowedTemplate(){
    assertTrue(isWindowedTemplate("-SSS-of-NNN-PPP-W"));
    assertTrue(isWindowedTemplate("-SSS-of-NNN-PPP-W"));
    assertTrue(isWindowedTemplate("-SSS-of-NNN-L"));
    assertTrue(isWindowedTemplate("-SSS-of-NNN-firstPane=F"));

    assertFalse(isWindowedTemplate("-SSS-of-NNN"));
    assertFalse(isWindowedTemplate("-SSS-of-lp"));
  }

  @Test
  public void testConstructWindowedName() {
    assertEquals("output-001-of-123.txt",
        constructName("output", "-SSS-of-NNN", ".txt", 1, 123, -1, false, false, null));

    assertEquals("output-001-of-123-PPP-W.txt",
        constructName("output", "-SSS-of-NNN-PPP-W", ".txt", 1, 123, -1, false, false, null));

    assertEquals("out.txt/part-00042-003-myWindowStr",
        constructName("out.txt", "/part-SSSSS-PPP-W", "", 42, 100, 3, false, false,
            "myWindowStr"));

    assertEquals("out.txt", constructName("ou", "t.t", "xt", 1, 1, 2, false, false,
        "anotherWindowStr"));

    assertEquals("out0102shard-oneMoreWindowStr-0013.txt",
        constructName("out", "SSNNshard-W-PPPP", ".txt", 1, 2, 13, false, false,
            "oneMoreWindowStr"));

    assertEquals("out-2/1.part-1-of-2-slidingWindow1-33-windowslidingWindow1-pane033.txt",
        constructName("out",
        "-N/S.part-S-of-N-W-PP-windowW-panePPP", ".txt", 1, 2, 33, false, false,
        "slidingWindow1"));

    // test first/last pane
    assertEquals("out.txt/part-00042-003-myWindowStr-last=false-first=false",
        constructName("out.txt", "/part-SSSSS-PPP-W-last=L-first=F", "", 42, 100, 3, false,
            false, "myWindowStr"));

    assertEquals("out.txt", constructName("ou", "t.t", "xt", 1, 1, 2, true, true,
        "anotherWindowStr"));

    assertEquals("out0102shard-oneMoreWindowStr-first_true13-0013.txt",
        constructName("out", "SSNNshard-W-first_FP-PPPP", ".txt", 1, 2, 13, true, false,
            "oneMoreWindowStr"));

    assertEquals("out-2/1.part-1-of-2-sWindow1-winsWindow1-p033-lastpane=true-firstpane=true.txt",
        constructName("out",
        "-N/S.part-S-of-N-W-winW-pPPP-lastpane=L-firstpane=F", ".txt", 1, 2, 33,
        true, true, "sWindow1"));
  }

}
