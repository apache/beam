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
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
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

  private static DateTimeFormatter formatter = ISODateTimeFormat.hourMinute();
  private String filenamePrefix;

  public WriteOneFilePerWindow(String filenamePrefix) {
    this.filenamePrefix = filenamePrefix;
  }

  @Override
  public PDone expand(PCollection<String> input) {
    return input.apply(
        TextIO.Write.to(new PerWindowFiles(filenamePrefix)).withWindowedWrites().withNumShards(3));
  }

  /**
   * A {@link FilenamePolicy} produces a base file name for a write based on metadata about the data
   * being written. This always includes the shard number and the total number of shards. For
   * windowed writes, it also includes the window and pane index (a sequence number assigned to each
   * trigger firing).
   */
  public static class PerWindowFiles extends FilenamePolicy {

    private final String output;

    public PerWindowFiles(String output) {
      this.output = output;
    }

    @Override
    public ValueProvider<String> getBaseOutputFilenameProvider() {
      return StaticValueProvider.of(output);
    }

    public String   filenamePrefixForWindow(IntervalWindow window) {
      return String.format(
          "%s-%s-%s", output, formatter.print(window.start()), formatter.print(window.end()));
    }

    @Override
    public String windowedFilename(WindowedContext context) {
      IntervalWindow window = (IntervalWindow) context.getWindow();
      return String.format(
          "%s-%s-of-%s",
          filenamePrefixForWindow(window), context.getShardNumber(), context.getNumShards());
    }

    @Override
    public String unwindowedFilename(Context context) {
      throw new UnsupportedOperationException("Unsupported.");
    }
  }
}
