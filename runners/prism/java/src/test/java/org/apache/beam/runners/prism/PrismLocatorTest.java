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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrismLocator}. */
@RunWith(JUnit4.class)
public class PrismLocatorTest {

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
  public void givenVersionOverride_thenResolves() throws IOException {
    assertThat(Files.exists(DESTINATION_DIRECTORY)).isFalse();
    PrismPipelineOptions options = options();
    options.setPrismVersionOverride("2.57.0");
    PrismLocator underTest = new PrismLocator(options);
    String got = underTest.resolve();
    assertThat(got).contains(DESTINATION_DIRECTORY.toString());
    assertThat(got).contains("2.57.0");
    Path gotPath = Paths.get(got);
    assertThat(Files.exists(gotPath)).isTrue();
  }

  @Test
  public void givenHttpPrismLocationOption_thenResolves() throws IOException {
    assertThat(Files.exists(DESTINATION_DIRECTORY)).isFalse();
    PrismPipelineOptions options = options();
    options.setPrismLocation(
        "https://github.com/apache/beam/releases/download/v2.57.0/apache_beam-v2.57.0-prism-darwin-arm64.zip");
    PrismLocator underTest = new PrismLocator(options);
    String got = underTest.resolve();
    assertThat(got).contains(DESTINATION_DIRECTORY.toString());
    Path gotPath = Paths.get(got);
    assertThat(Files.exists(gotPath)).isTrue();
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
  public void givenGithubTagPrismLocationOption_thenThrows() {
    PrismPipelineOptions options = options();
    options.setPrismLocation(
        "https://github.com/apache/beam/releases/tag/v2.57.0/apache_beam-v2.57.0-prism-darwin-amd64.zip");
    PrismLocator underTest = new PrismLocator(options);
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, underTest::resolve);
    assertThat(error.getMessage())
        .contains(
            "Provided --prismLocation URL is not an Apache Beam Github Release page URL or download URL");
  }

  @Test
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
