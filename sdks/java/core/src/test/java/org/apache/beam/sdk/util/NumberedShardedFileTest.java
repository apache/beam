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
package org.apache.beam.sdk.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link NumberedShardedFile}. */
@RunWith(JUnit4.class)
public class NumberedShardedFileTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ExpectedException thrown = ExpectedException.none();
  private Sleeper fastClock =
      millis -> {
        // No sleep.
      };

  private final BackOff backOff = NumberedShardedFile.BACK_OFF_FACTORY.backoff();
  private String filePattern;

  @Before
  public void setup() throws IOException {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10729
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    filePattern =
        LocalResources.fromFile(tmpFolder.getRoot(), true)
            .resolve("*", StandardResolveOptions.RESOLVE_FILE)
            .toString();
  }

  @Test
  public void testPreconditionFilePathIsNull() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Expected valid file path, but received"));
    new NumberedShardedFile(null);
  }

  @Test
  public void testPreconditionFilePathIsEmpty() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Expected valid file path, but received"));
    new NumberedShardedFile("");
  }

  @Test
  public void testReadMultipleShards() throws Exception {
    String contents1 = "To be or not to be, ",
        contents2 = "it is not a question.",
        contents3 = "should not be included";

    File tmpFile1 = tmpFolder.newFile("result-000-of-002");
    File tmpFile2 = tmpFolder.newFile("result-001-of-002");
    File tmpFile3 = tmpFolder.newFile("tmp");
    Files.write(contents1, tmpFile1, StandardCharsets.UTF_8);
    Files.write(contents2, tmpFile2, StandardCharsets.UTF_8);
    Files.write(contents3, tmpFile3, StandardCharsets.UTF_8);

    filePattern =
        LocalResources.fromFile(tmpFolder.getRoot(), true)
            .resolve("result-*", StandardResolveOptions.RESOLVE_FILE)
            .toString();
    NumberedShardedFile shardedFile = new NumberedShardedFile(filePattern);

    assertThat(shardedFile.readFilesWithRetries(), containsInAnyOrder(contents1, contents2));
  }

  @Test
  public void testReadEmpty() throws Exception {
    File emptyFile = tmpFolder.newFile("result-000-of-001");
    Files.write("", emptyFile, StandardCharsets.UTF_8);
    NumberedShardedFile shardedFile = new NumberedShardedFile(filePattern);

    assertThat(shardedFile.readFilesWithRetries(), empty());
  }

  @Test
  public void testReadCustomTemplate() throws Exception {
    String contents1 = "To be or not to be, ", contents2 = "it is not a question.";

    // Customized template: resultSSS-totalNNN
    File tmpFile1 = tmpFolder.newFile("result0-total2");
    File tmpFile2 = tmpFolder.newFile("result1-total2");
    Files.write(contents1, tmpFile1, StandardCharsets.UTF_8);
    Files.write(contents2, tmpFile2, StandardCharsets.UTF_8);

    Pattern customizedTemplate =
        Pattern.compile("(?x) result (?<shardnum>\\d+) - total (?<numshards>\\d+)");
    NumberedShardedFile shardedFile = new NumberedShardedFile(filePattern, customizedTemplate);

    assertThat(shardedFile.readFilesWithRetries(), containsInAnyOrder(contents1, contents2));
  }

  @Test
  public void testReadWithRetriesFailsWhenTemplateIncorrect() throws Exception {
    File tmpFile = tmpFolder.newFile();
    Files.write("Test for file checksum verifier.", tmpFile, StandardCharsets.UTF_8);

    NumberedShardedFile shardedFile =
        new NumberedShardedFile(filePattern, Pattern.compile("incorrect-template"));

    thrown.expect(IOException.class);
    thrown.expectMessage(
        containsString(
            "Unable to read file(s) after retrying " + NumberedShardedFile.MAX_READ_RETRIES));
    shardedFile.readFilesWithRetries(fastClock, backOff);
  }

  @Test
  public void testReadWithRetriesFailsSinceFilesystemError() throws Exception {
    File tmpFile = tmpFolder.newFile();
    Files.write("Test for file checksum verifier.", tmpFile, StandardCharsets.UTF_8);
    NumberedShardedFile shardedFile = spy(new NumberedShardedFile(filePattern));
    doThrow(IOException.class).when(shardedFile).readLines(anyCollection());

    thrown.expect(IOException.class);
    thrown.expectMessage(
        containsString(
            "Unable to read file(s) after retrying " + NumberedShardedFile.MAX_READ_RETRIES));
    shardedFile.readFilesWithRetries(fastClock, backOff);
  }

  @Test
  public void testReadWithRetriesFailsWhenOutputDirEmpty() throws Exception {
    NumberedShardedFile shardedFile = new NumberedShardedFile(filePattern);

    thrown.expect(IOException.class);
    thrown.expectMessage(
        containsString(
            "Unable to read file(s) after retrying " + NumberedShardedFile.MAX_READ_RETRIES));
    shardedFile.readFilesWithRetries(fastClock, backOff);
  }

  @Test
  public void testReadWithRetriesFailsWhenRedundantFileLoaded() throws Exception {
    tmpFolder.newFile("result-000-of-001");
    tmpFolder.newFile("tmp-result-000-of-001");

    NumberedShardedFile shardedFile = new NumberedShardedFile(filePattern);

    thrown.expect(IOException.class);
    thrown.expectMessage(
        containsString(
            "Unable to read file(s) after retrying " + NumberedShardedFile.MAX_READ_RETRIES));
    shardedFile.readFilesWithRetries(fastClock, backOff);
  }
}
