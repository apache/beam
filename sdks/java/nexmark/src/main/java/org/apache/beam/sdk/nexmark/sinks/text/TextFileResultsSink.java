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

package org.apache.beam.sdk.nexmark.sinks.text;

import com.google.common.base.Strings;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Sink which writes formatted results to a text file.
 */
public class TextFileResultsSink {
  /**
   * Send {@code formattedResults} to text files.
   */
  public static PTransform<PCollection<String>, PDone> createSink(
      NexmarkOptions options,
      String queryName,
      long now) {

    String filename = textFilename(options, now, queryName);
    NexmarkUtils.console("Writing results to text files at %s", filename);
    return TextIO.write().to(filename);
  }

  /**
   * Return a file name for plain text.
   */
  private static String textFilename(NexmarkOptions options, long now, String queryName) {
    String baseFilename = options.getOutputPath();
    if (Strings.isNullOrEmpty(baseFilename)) {
      throw new RuntimeException("Missing --outputPath");
    }
    switch (options.getResourceNameMode()) {
      case VERBATIM:
        return baseFilename;
      case QUERY:
        return String.format("%s/nexmark_%s.txt", baseFilename, queryName);
      case QUERY_AND_SALT:
        return String.format("%s/nexmark_%s_%d.txt", baseFilename, queryName, now);
    }
    throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
  }
}
