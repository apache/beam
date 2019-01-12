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
package org.apache.beam.sdk.testing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.vendor.guava.v20_0.com.google.common.io.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;

/** Tests for {@link FileChecksumMatcher}. */
@RunWith(JUnit4.class)
public class FileChecksumMatcherTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private PipelineResult pResult = Mockito.mock(PipelineResult.class);

  @Test
  public void testPreconditionChecksumIsNull() throws IOException {
    String tmpPath = tmpFolder.newFile().getPath();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Expected valid checksum, but received"));
    new FileChecksumMatcher(null, tmpPath);
  }

  @Test
  public void testPreconditionChecksumIsEmpty() throws IOException {
    String tmpPath = tmpFolder.newFile().getPath();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Expected valid checksum, but received"));
    new FileChecksumMatcher("", tmpPath);
  }

  @Test
  public void testPreconditionFilePathIsEmpty() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Expected valid file path, but received"));
    new FileChecksumMatcher("checksumString", "");
  }

  @Test
  public void testPreconditionShardTemplateIsNull() throws IOException {
    String tmpPath = tmpFolder.newFile().getPath();

    thrown.expect(NullPointerException.class);
    thrown.expectMessage(
        containsString(
            "Expected non-null shard pattern. "
                + "Please call the other constructor to use default pattern:"));
    new FileChecksumMatcher("checksumString", tmpPath, null);
  }

  @Test
  public void testMatcherThatVerifiesSingleFile() throws IOException {
    File tmpFile = tmpFolder.newFile("result-000-of-001");
    Files.write("Test for file checksum verifier.", tmpFile, StandardCharsets.UTF_8);
    FileChecksumMatcher matcher =
        new FileChecksumMatcher("a8772322f5d7b851777f820fc79d050f9d302915", tmpFile.getPath());

    assertThat(pResult, matcher);
  }

  @Test
  public void testMatcherThatVerifiesMultipleFiles() throws IOException {
    File tmpFile1 = tmpFolder.newFile("result-000-of-002");
    File tmpFile2 = tmpFolder.newFile("result-001-of-002");
    File tmpFile3 = tmpFolder.newFile("tmp");
    Files.write("To be or not to be, ", tmpFile1, StandardCharsets.UTF_8);
    Files.write("it is not a question.", tmpFile2, StandardCharsets.UTF_8);
    Files.write("tmp", tmpFile3, StandardCharsets.UTF_8);

    FileChecksumMatcher matcher =
        new FileChecksumMatcher(
            "90552392c28396935fe4f123bd0b5c2d0f6260c8",
            tmpFolder.getRoot().toPath().resolve("result-*").toString());

    assertThat(pResult, matcher);
  }

  @Test
  public void testMatcherThatVerifiesFileWithEmptyContent() throws IOException {
    File emptyFile = tmpFolder.newFile("result-000-of-001");
    Files.write("", emptyFile, StandardCharsets.UTF_8);
    FileChecksumMatcher matcher =
        new FileChecksumMatcher(
            "da39a3ee5e6b4b0d3255bfef95601890afd80709",
            tmpFolder.getRoot().toPath().resolve("*").toString());

    assertThat(pResult, matcher);
  }

  @Test
  public void testMatcherThatUsesCustomizedTemplate() throws Exception {
    // Customized template: resultSSS-totalNNN
    File tmpFile1 = tmpFolder.newFile("result0-total2");
    File tmpFile2 = tmpFolder.newFile("result1-total2");
    Files.write("To be or not to be, ", tmpFile1, StandardCharsets.UTF_8);
    Files.write("it is not a question.", tmpFile2, StandardCharsets.UTF_8);

    Pattern customizedTemplate =
        Pattern.compile("(?x) result (?<shardnum>\\d+) - total (?<numshards>\\d+)");
    FileChecksumMatcher matcher =
        new FileChecksumMatcher(
            "90552392c28396935fe4f123bd0b5c2d0f6260c8",
            tmpFolder.getRoot().toPath().resolve("*").toString(),
            customizedTemplate);

    assertThat(pResult, matcher);
  }
}
