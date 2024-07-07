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
import static org.apache.beam.runners.prism.PrismRunnerTest.getLocalPrismBuildOrIgnoreTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrismLocator}. */
@RunWith(JUnit4.class)
public class PrismLocatorTest {

  @Test
  public void givenVersionOverride_thenResolves() throws IOException {
    PrismPipelineOptions options = options();
    options.setPrismVersionOverride("2.57.0");
    PrismLocator underTest = new PrismLocator(options);
    String got = underTest.resolve();
    assertThat(got).contains(".apache_beam/cache/prism/bin/");
    assertThat(got).contains("2.57.0");
    Path gotPath = Paths.get(got);
    assertThat(Files.exists(gotPath)).isTrue();
    Files.delete(gotPath);
  }

  @Test
  public void givenHttpPrismLocationOption_thenResolves() throws IOException {
    PrismPipelineOptions options = options();
    options.setPrismLocation(
        "https://github.com/apache/beam/releases/download/v2.57.0/apache_beam-v2.57.0-prism-darwin-arm64.zip");
    PrismLocator underTest = new PrismLocator(options);
    String got = underTest.resolve();
    assertThat(got).contains(".apache_beam/cache/prism/bin/");
    Path gotPath = Paths.get(got);
    assertThat(Files.exists(gotPath)).isTrue();
    Files.delete(gotPath);
  }

  @Test
  public void givenFilePrismLocationOption_thenResolves() throws IOException {
    PrismPipelineOptions options = options();
    options.setPrismLocation(getLocalPrismBuildOrIgnoreTest());
    PrismLocator underTest = new PrismLocator(options);
    String got = underTest.resolve();
    assertThat(got).contains(".apache_beam/cache/prism/bin/");
    Path gotPath = Paths.get(got);
    assertThat(Files.exists(gotPath)).isTrue();
    Files.delete(gotPath);
  }

  private static PrismPipelineOptions options() {
    return PipelineOptionsFactory.create().as(PrismPipelineOptions.class);
  }
}
