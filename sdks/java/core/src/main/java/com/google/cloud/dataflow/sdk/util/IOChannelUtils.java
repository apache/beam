/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides utilities for creating read and write channels.
 */
public class IOChannelUtils {
  // TODO: add registration mechanism for adding new schemas.
  private static final Map<String, IOChannelFactory> FACTORY_MAP =
      Collections.synchronizedMap(new HashMap<String, IOChannelFactory>());

  // Pattern that matches shard placeholders within a shard template.
  private static final Pattern SHARD_FORMAT_RE = Pattern.compile("(S+|N+)");

  /**
   * Associates a scheme with an {@link IOChannelFactory}.
   *
   * <p>The given factory is used to construct read and write channels when
   * a URI is provided with the given scheme.
   *
   * <p>For example, when reading from "gs://bucket/path", the scheme "gs" is
   * used to lookup the appropriate factory.
   */
  public static void setIOFactory(String scheme, IOChannelFactory factory) {
    FACTORY_MAP.put(scheme, factory);
  }

  /**
   * Registers standard factories globally. This requires {@link PipelineOptions}
   * to provide, e.g., credentials for GCS.
   */
  public static void registerStandardIOFactories(PipelineOptions options) {
    setIOFactory("gs", new GcsIOChannelFactory(options.as(GcsOptions.class)));
  }

  /**
   * Creates a write channel for the given filename.
   */
  public static WritableByteChannel create(String filename, String mimeType)
      throws IOException {
    return getFactory(filename).create(filename, mimeType);
  }

  /**
   * Creates a write channel for the given file components.
   *
   * <p>If numShards is specified, then a ShardingWritableByteChannel is
   * returned.
   *
   * <p>Shard numbers are 0 based, meaning they start with 0 and end at the
   * number of shards - 1.
   */
  public static WritableByteChannel create(String prefix, String shardTemplate,
      String suffix, int numShards, String mimeType) throws IOException {
    if (numShards == 1) {
      return create(constructName(prefix, shardTemplate, suffix, 0, 1),
                    mimeType);
    }

    // It is the callers responsibility to close this channel.
    @SuppressWarnings("resource")
    ShardingWritableByteChannel shardingChannel =
        new ShardingWritableByteChannel();

    Set<String> outputNames = new HashSet<>();
    for (int i = 0; i < numShards; i++) {
      String outputName =
          constructName(prefix, shardTemplate, suffix, i, numShards);
      if (!outputNames.add(outputName)) {
        throw new IllegalArgumentException(
            "Shard name collision detected for: " + outputName);
      }
      WritableByteChannel channel = create(outputName, mimeType);
      shardingChannel.addChannel(channel);
    }

    return shardingChannel;
  }

  /**
   * Returns the size in bytes for the given specification.
   *
   * <p>The specification is not expanded; it is used verbatim.
   *
   * <p>{@link FileNotFoundException} will be thrown if the resource does not exist.
   */
  public static long getSizeBytes(String spec) throws IOException {
    return getFactory(spec).getSizeBytes(spec);
  }

  /**
   * Constructs a fully qualified name from components.
   *
   * <p>The name is built from a prefix, shard template (with shard numbers
   * applied), and a suffix.  All components are required, but may be empty
   * strings.
   *
   * <p>Within a shard template, repeating sequences of the letters "S" or "N"
   * are replaced with the shard number, or number of shards respectively.  The
   * numbers are formatted with leading zeros to match the length of the
   * repeated sequence of letters.
   *
   * <p>For example, if prefix = "output", shardTemplate = "-SSS-of-NNN", and
   * suffix = ".txt", with shardNum = 1 and numShards = 100, the following is
   * produced:  "output-001-of-100.txt".
   */
  public static String constructName(String prefix,
      String shardTemplate, String suffix, int shardNum, int numShards) {
    // Matcher API works with StringBuffer, rather than StringBuilder.
    StringBuffer sb = new StringBuffer();
    sb.append(prefix);

    Matcher m = SHARD_FORMAT_RE.matcher(shardTemplate);
    while (m.find()) {
      boolean isShardNum = (m.group(1).charAt(0) == 'S');

      char[] zeros = new char[m.end() - m.start()];
      Arrays.fill(zeros, '0');
      DecimalFormat df = new DecimalFormat(String.valueOf(zeros));
      String formatted = df.format(isShardNum
                                   ? shardNum
                                   : numShards);
      m.appendReplacement(sb, formatted);
    }
    m.appendTail(sb);

    sb.append(suffix);
    return sb.toString();
  }

  private static final Pattern URI_SCHEME_PATTERN = Pattern.compile(
      "(?<scheme>[a-zA-Z][-a-zA-Z0-9+.]*)://.*");

  /**
   * Returns the IOChannelFactory associated with an input specification.
   */
  public static IOChannelFactory getFactory(String spec) throws IOException {
    // The spec is almost, but not quite, a URI. In particular,
    // the reserved characters '[', ']', and '?' have meanings that differ
    // from their use in the URI spec. ('*' is not reserved).
    // Here, we just need the scheme, which is so circumscribed as to be
    // very easy to extract with a regex.
    Matcher matcher = URI_SCHEME_PATTERN.matcher(spec);

    if (!matcher.matches()) {
      return new FileIOChannelFactory();
    }

    String scheme = matcher.group("scheme");
    IOChannelFactory ioFactory = FACTORY_MAP.get(scheme);
    if (ioFactory != null) {
      return ioFactory;
    }

    throw new IOException("Unable to find handler for " + spec);
  }

  /**
   * Resolve the given {@code other} against the {@code path}.
   *
   * <p>If the {@code other} parameter is an absolute path then this method trivially returns
   * other. If {@code other} is an empty path then this method trivially returns the given
   * {@code path}. Otherwise this method considers the given {@code path} to be a directory and
   * resolves the {@code other} path against this path. In the simplest case, the {@code other}
   * path does not have a root component, in which case this method joins the {@code other} path
   * to the given {@code path} and returns a resulting path that ends with the {@code other} path.
   * Where the {@code other} path has a root component then resolution is highly implementation
   * dependent and therefore unspecified.
   */
  public static String resolve(String path, String other) throws IOException {
    return getFactory(path).resolve(path, other);
  }
}
