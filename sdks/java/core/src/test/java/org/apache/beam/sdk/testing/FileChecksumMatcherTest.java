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

import static org.apache.beam.sdk.testing.FileChecksumMatcher.fileContentsHaveChecksum;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import org.apache.beam.sdk.util.NumberedShardedFile;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FileChecksumMatcher}. */
@RunWith(JUnit4.class)
public class FileChecksumMatcherTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testPreconditionChecksumIsNull() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Expected valid checksum, but received"));
    fileContentsHaveChecksum(null);
  }

  @Test
  public void testPreconditionChecksumIsEmpty() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Expected valid checksum, but received"));
    fileContentsHaveChecksum("");
  }

  @Test
  public void testMatcherThatVerifiesSingleFile() throws IOException {
    File tmpFile = tmpFolder.newFile("result-000-of-001");
    Files.write("Test for file checksum verifier.", tmpFile, StandardCharsets.UTF_8);

    assertThat(
        new NumberedShardedFile(tmpFile.getPath()),
        fileContentsHaveChecksum("a8772322f5d7b851777f820fc79d050f9d302915"));
  }

  @Test
  public void testMatcherThatVerifiesMultipleFiles() throws IOException {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10747
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    File tmpFile1 = tmpFolder.newFile("result-000-of-002");
    File tmpFile2 = tmpFolder.newFile("result-001-of-002");
    File tmpFile3 = tmpFolder.newFile("tmp");
    Files.write("To be or not to be, ", tmpFile1, StandardCharsets.UTF_8);
    Files.write("it is not a question.", tmpFile2, StandardCharsets.UTF_8);
    Files.write("tmp", tmpFile3, StandardCharsets.UTF_8);

    assertThat(
        new NumberedShardedFile(tmpFolder.getRoot().toPath().resolve("result-*").toString()),
        fileContentsHaveChecksum("90552392c28396935fe4f123bd0b5c2d0f6260c8"));
  }

  @Test
  public void testMatcherThatVerifiesFileWithEmptyContent() throws IOException {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10748
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    File emptyFile = tmpFolder.newFile("result-000-of-001");
    Files.write("", emptyFile, StandardCharsets.UTF_8);

    assertThat(
        new NumberedShardedFile(tmpFolder.getRoot().toPath().resolve("*").toString()),
        fileContentsHaveChecksum("da39a3ee5e6b4b0d3255bfef95601890afd80709"));
  }

  @Test
  public void testMatcherThatUsesCustomizedTemplate() throws Exception {
    // Customized template: resultSSS-totalNNN
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10749
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    File tmpFile1 = tmpFolder.newFile("result0-total2");
    File tmpFile2 = tmpFolder.newFile("result1-total2");
    Files.write("To be or not to be, ", tmpFile1, StandardCharsets.UTF_8);
    Files.write("it is not a question.", tmpFile2, StandardCharsets.UTF_8);

    Pattern customizedTemplate =
        Pattern.compile("(?x) result (?<shardnum>\\d+) - total (?<numshards>\\d+)");

    assertThat(
        new NumberedShardedFile(
            tmpFolder.getRoot().toPath().resolve("*").toString(), customizedTemplate),
        fileContentsHaveChecksum("90552392c28396935fe4f123bd0b5c2d0f6260c8"));
  }
}
