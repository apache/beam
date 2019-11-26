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
package org.apache.beam.sdk.io.common;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Contains helper methods for file based IO Integration tests. */
public class FileBasedIOITHelper {

  private FileBasedIOITHelper() {}

  public static FileBasedIOTestPipelineOptions readFileBasedIOITPipelineOptions() {
    return IOITHelper.readIOTestPipelineOptions(FileBasedIOTestPipelineOptions.class);
  }

  public static String appendTimestampSuffix(String text) {
    return String.format("%s_%s", text, new Date().getTime());
  }

  /** Constructs text lines in files used for testing. */
  public static class DeterministicallyConstructTestTextLineFn extends DoFn<Long, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(String.format("IO IT Test line of text. Line seed: %s", c.element()));
    }
  }

  /** Deletes matching files using the FileSystems API. */
  public static class DeleteFileFn extends DoFn<String, Void> {

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      MatchResult match =
          Iterables.getOnlyElement(FileSystems.match(Collections.singletonList(c.element())));

      Set<ResourceId> resourceIds = new HashSet<>();
      for (MatchResult.Metadata metadataElem : match.metadata()) {
        resourceIds.add(metadataElem.resourceId());
      }

      FileSystems.delete(resourceIds);
    }
  }
}
