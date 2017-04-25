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
package org.apache.beam.examples.common;

import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * A {@link DoFn} that writes elements to files with names deterministically derived from the lower
 * and upper bounds of their key (an {@link IntervalWindow}).
 *
 * <p>This is test utility code, not for end-users, so examples can be focused on their primary
 * lessons.
 */
public class WriteOneFilePerWindow extends PTransform<PCollection<String>, PDone> {

  private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();
  private final String filenamePrefix;

  public WriteOneFilePerWindow(String filenamePrefix) {
    this.filenamePrefix = filenamePrefix;
  }

  @Override
  public PDone expand(PCollection<String> input) {
    return input.apply(
        TextIO.write()
            .to(filenamePrefix)
            .withFilenamePolicy(new PerWindowFiles(filenamePrefix))
            .withWindowedWrites()
            .withNumShards(3));
  }

  /**
   * A {@link FilenamePolicy} produces a base file name for a write based on metadata about the data
   * being written. This always includes the shard number and the total number of shards. For
   * windowed writes, it also includes the window and pane index (a sequence number assigned to each
   * trigger firing).
   */
  public static class PerWindowFiles extends FilenamePolicy {

    private final String filenamePrefix;

    public PerWindowFiles(String filenamePrefix) {
      String filePrefixOnly;
      try {
        ResourceId file = FileSystems.matchNewResource(filenamePrefix, false /* isDirectory */);
        filePrefixOnly = file.getFilename();
      } catch (Exception e) {
        filePrefixOnly = "";
      }
      this.filenamePrefix = filePrefixOnly;
    }

    public String filenamePrefixForWindow(IntervalWindow window) {
      return String.format("%s-%s-%s",
          filenamePrefix, FORMATTER.print(window.start()), FORMATTER.print(window.end()));
    }

    @Override
    public ResourceId windowedFilename(
        ResourceId outputDirectory, WindowedContext context, String extension) {
      IntervalWindow window = (IntervalWindow) context.getWindow();
      String filename = String.format(
          "%s-%s-of-%s%s",
          filenamePrefixForWindow(window), context.getShardNumber(), context.getNumShards(),
          extension);
      return outputDirectory.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
    }

    @Override
    public ResourceId unwindowedFilename(
        ResourceId outputDirectory, Context context, String extension) {
      throw new UnsupportedOperationException("Unsupported.");
    }
  }
}
