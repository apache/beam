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
package org.apache.beam.runners.prism;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.runners.prism.PrismLocator.prismBinDirectory;
import static org.apache.beam.runners.prism.PrismRunnerTest.getLocalPrismBuildOrIgnoreTest;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrismLocator}. */
@RunWith(JUnit4.class)
public class PrismLocatorTest {

  private static final ReleaseInfo RELEASE_INFO = ReleaseInfo.getReleaseInfo();
  private static final Path DESTINATION_DIRECTORY = prismBinDirectory();

  @Before
  public void setup() throws IOException {
    if (Files.exists(DESTINATION_DIRECTORY)) {
      Files.walkFileTree(
          DESTINATION_DIRECTORY,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Files.delete(file);
              return FileVisitResult.CONTINUE;
            }
          });

      Files.delete(DESTINATION_DIRECTORY);
    }
  }

  @Test
  public void givenVersionOverride_thenResolvesLocation() throws IOException {
    PrismPipelineOptions options = options();
    options.setPrismVersionOverride("2.57.0-RC1");

    PrismLocator underTest = new PrismLocator(options);
    String got = underTest.resolveLocation();

    assertThat(got)
        .contains(
            "https://github.com/apache/beam/releases/download/v" + RELEASE_INFO.getSdkVersion());
    assertThat(got).contains("apache_beam-v2.57.0-RC1-prism");
    assertThat(got).contains(".zip");
  }

  // This testcase validates a user override to download a different release version specifically.
  @Test
  public void givenHttpPrismLocationOption_thenResolvesLocation() throws IOException {
    PrismPipelineOptions options = options();
    String want =
        "https://github.com/apache/beam/releases/download/v2.57.0/apache_beam-v2.57.0-prism-darwin-arm64.zip";
    options.setPrismLocation(want);

    PrismLocator underTest = new PrismLocator(options);
    String got = underTest.resolveLocation();

    assertThat(got).isEqualTo(want);
  }

  // This testcase is the Release Validation behavior, where we provide an RC option, but
  // need to resolve the download for the non-RC version.
  // Copy the URL directly, and set the location, override the file's RC version with the final
  // version.
  @Test
  public void givenRCGithubTagPrismLocationOption_thenResolvesLocation() {
    PrismPipelineOptions options = options();
    options.setPrismLocation("https://github.com/apache/beam/releases/tag/v2.57.0-RC1/");
    options.setPrismVersionOverride("2.57.0");

    PrismLocator underTest = new PrismLocator(options);
    String got = underTest.resolveLocation();

    assertThat(got)
        .contains(
            "https://github.com/apache/beam/releases/download/v2.57.0-RC1/apache_beam-v2.57.0-prism");
    assertThat(got).contains(".zip");
  }

  @Test
  public void givenRCGithubTagPrismLocationOptionNoTrailingSlash_thenResolvesLocation() {
    PrismPipelineOptions options = options();
    options.setPrismLocation("https://github.com/apache/beam/releases/tag/v2.57.0-RC2");
    options.setPrismVersionOverride("2.57.0");

    PrismLocator underTest = new PrismLocator(options);
    String got = underTest.resolveLocation();

    assertThat(got)
        .contains(
            "https://github.com/apache/beam/releases/download/v2.57.0-RC2/apache_beam-v2.57.0-prism");
    assertThat(got).contains(".zip");
  }

  @Test
  public void givenFilePrismLocationOption_thenResolves() throws IOException {
    assertThat(Files.exists(DESTINATION_DIRECTORY)).isFalse();
    PrismPipelineOptions options = options();
    options.setPrismLocation(getLocalPrismBuildOrIgnoreTest());

    PrismLocator underTest = new PrismLocator(options);
    String got = underTest.resolve();

    assertThat(got).contains(DESTINATION_DIRECTORY.toString());
    Path gotPath = Paths.get(got);
    assertThat(Files.exists(gotPath)).isTrue();
  }

  @Test
  public void givenIncorrectGithubPrismLocationOption_thenThrows() {
    PrismPipelineOptions options = options();
    // This is an incorrect github download path. Downloads are under /download/ not tag.
    options.setPrismLocation(
        "https://github.com/apache/beam/releases/tag/v2.57.0/apache_beam-v2.57.0-prism-darwin-amd64.zip");

    PrismLocator underTest = new PrismLocator(options);

    RuntimeException error = assertThrows(RuntimeException.class, underTest::resolve);
    // Message should contain the incorrectly constructed download link.
    assertThat(error.getMessage()).contains(".zip/apache_beam");
  }

  @Test
  @Ignore // TODO: use mock site. Currently failing with response code 500 instead of 404
  public void givenPrismLocation404_thenThrows() {
    PrismPipelineOptions options = options();
    options.setPrismLocation("https://example.com/i/dont/exist.zip");

    PrismLocator underTest = new PrismLocator(options);
    RuntimeException error = assertThrows(RuntimeException.class, underTest::resolve);
    assertThat(error.getMessage()).contains("NotFoundException");
  }

  private static PrismPipelineOptions options() {
    return PipelineOptionsFactory.create().as(PrismPipelineOptions.class);
  }
}
