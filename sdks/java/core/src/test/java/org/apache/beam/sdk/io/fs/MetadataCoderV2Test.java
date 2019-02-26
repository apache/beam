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
package org.apache.beam.sdk.io.fs;

import java.nio.file.Path;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link MetadataCoderV2}. */
public class MetadataCoderV2Test {

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testEncodeDecodeWithDefaultLastModifiedMills() throws Exception {
    Path filePath = tmpFolder.newFile("somefile").toPath();
    Metadata metadata =
        Metadata.builder()
            .setResourceId(
                FileSystems.matchNewResource(filePath.toString(), false /* isDirectory */))
            .setIsReadSeekEfficient(true)
            .setSizeBytes(1024)
            .build();
    CoderProperties.coderDecodeEncodeEqual(MetadataCoderV2.of(), metadata);
  }

  @Test
  public void testEncodeDecodeWithCustomLastModifiedMills() throws Exception {
    Path filePath = tmpFolder.newFile("somefile").toPath();
    Metadata metadata =
        Metadata.builder()
            .setResourceId(
                FileSystems.matchNewResource(filePath.toString(), false /* isDirectory */))
            .setIsReadSeekEfficient(true)
            .setSizeBytes(1024)
            .setLastModifiedMillis(1541097000L)
            .build();
    CoderProperties.coderDecodeEncodeEqual(MetadataCoderV2.of(), metadata);
  }

  @Test
  public void testCoderSerializable() {
    CoderProperties.coderSerializable(MetadataCoderV2.of());
  }
}
