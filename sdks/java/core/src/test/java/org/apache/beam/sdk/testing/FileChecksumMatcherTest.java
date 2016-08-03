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

import org.apache.beam.sdk.PipelineResult;

import com.google.common.io.Files;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Tests for {@link FileChecksumMatcher}. */
@RunWith(JUnit4.class)
public class FileChecksumMatcherTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Mock
  private PipelineResult pResult = Mockito.mock(PipelineResult.class);

  @Test
  public void testPreconditionValidChecksumString() throws IOException{
    String tmpPath = tmpFolder.newFile().getPath();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Expected valid checksum, but received"));
    new FileChecksumMatcher(null, tmpPath);
    new FileChecksumMatcher("", tmpPath);
  }

  @Test
  public void testPreconditionValidFilePath() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Expected valid file path, but received"));
    new FileChecksumMatcher("checksumString", null);
    new FileChecksumMatcher("checksumString", "");
  }

  @Test
  public void testChecksumVerify() throws IOException{
    File tmpFile = tmpFolder.newFile();
    Files.write("Test for file checksum verifier.", tmpFile, StandardCharsets.UTF_8);
    FileChecksumMatcher matcher =
        new FileChecksumMatcher("a8772322f5d7b851777f820fc79d050f9d302915", tmpFile.getPath());

    assertThat(pResult, matcher);
  }
 }
