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

import com.google.common.annotations.VisibleForTesting;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.values.KV;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * A {@link DoFn} that writes elements to files with names deterministically derived from the lower
 * and upper bounds of their key (an {@link IntervalWindow}).
 *
 * <p>This is test utility code, not for end-users, so examples can be focused
 * on their primary lessons.
 */
public class WriteWindowedFilesDoFn
    extends DoFn<KV<IntervalWindow, Iterable<KV<String, Long>>>, Void> {

  static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
  static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  private static DateTimeFormatter formatter = ISODateTimeFormat.hourMinute();

  private final String output;

  public WriteWindowedFilesDoFn(String output) {
    this.output = output;
  }

  @VisibleForTesting
  public static String fileForWindow(String output, IntervalWindow window) {
    return String.format(
        "%s-%s-%s", output, formatter.print(window.start()), formatter.print(window.end()));
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws Exception {
    // Build a file name from the window
    IntervalWindow window = context.element().getKey();
    String outputShard = fileForWindow(output, window);

    // Open the file and write all the values
    IOChannelFactory factory = IOChannelUtils.getFactory(outputShard);
    OutputStream out = Channels.newOutputStream(factory.create(outputShard, "text/plain"));
    for (KV<String, Long> wordCount : context.element().getValue()) {
      STRING_CODER.encode(
          wordCount.getKey() + ": " + wordCount.getValue(), out, Coder.Context.OUTER);
      out.write(NEWLINE);
    }
    out.close();
  }
}
