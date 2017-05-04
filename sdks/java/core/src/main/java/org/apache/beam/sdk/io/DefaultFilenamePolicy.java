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
package org.apache.beam.sdk.io;

import static com.google.common.base.MoreObjects.firstNonNull;

import com.google.common.annotations.VisibleForTesting;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;

/**
 * A default {@link FilenamePolicy} for unwindowed files. This policy is constructed using three
 * parameters that together define the output name of a sharded file, in conjunction with the number
 * of shards and index of the particular file, using {@link #constructName}.
 *
 * <p>Most users of unwindowed files will use this {@link DefaultFilenamePolicy}. For more advanced
 * uses in generating different files for each window and other sharding controls, see the
 * {@code WriteOneFilePerWindow} example pipeline.
 */
public final class DefaultFilenamePolicy extends FilenamePolicy {
  /** The default sharding name template used in {@link #constructUsingStandardParameters}. */
  public static final String DEFAULT_SHARD_TEMPLATE = ShardNameTemplate.INDEX_OF_MAX;

  // Pattern that matches shard placeholders within a shard template.
  private static final Pattern SHARD_FORMAT_RE = Pattern.compile("(S+|N+)");

  /**
   * Constructs a new {@link DefaultFilenamePolicy}.
   *
   * @see DefaultFilenamePolicy for more information on the arguments to this function.
   */
  @VisibleForTesting
  DefaultFilenamePolicy(ValueProvider<String> prefix, String shardTemplate, String suffix) {
    this.prefix = prefix;
    this.shardTemplate = shardTemplate;
    this.suffix = suffix;
  }

  /**
   * A helper function to construct a {@link DefaultFilenamePolicy} using the standard filename
   * parameters, namely a provided {@link ResourceId} for the output prefix, and possibly-null
   * shard name template and suffix.
   *
   * <p>Any filename component of the provided resource will be used as the filename prefix.
   *
   * <p>If provided, the shard name template will be used; otherwise {@link #DEFAULT_SHARD_TEMPLATE}
   * will be used.
   *
   * <p>If provided, the suffix will be used; otherwise the files will have an empty suffix.
   */
  public static DefaultFilenamePolicy constructUsingStandardParameters(
      ValueProvider<ResourceId> outputPrefix,
      @Nullable String shardTemplate,
      @Nullable String filenameSuffix) {
    return new DefaultFilenamePolicy(
        NestedValueProvider.of(outputPrefix, new ExtractFilename()),
        firstNonNull(shardTemplate, DEFAULT_SHARD_TEMPLATE),
        firstNonNull(filenameSuffix, ""));
  }

  private final ValueProvider<String> prefix;
  private final String shardTemplate;
  private final String suffix;

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
  public static String constructName(
      String prefix, String shardTemplate, String suffix, int shardNum, int numShards) {
    // Matcher API works with StringBuffer, rather than StringBuilder.
    StringBuffer sb = new StringBuffer();
    sb.append(prefix);

    Matcher m = SHARD_FORMAT_RE.matcher(shardTemplate);
    while (m.find()) {
      boolean isShardNum = (m.group(1).charAt(0) == 'S');

      char[] zeros = new char[m.end() - m.start()];
      Arrays.fill(zeros, '0');
      DecimalFormat df = new DecimalFormat(String.valueOf(zeros));
      String formatted = df.format(isShardNum ? shardNum : numShards);
      m.appendReplacement(sb, formatted);
    }
    m.appendTail(sb);

    sb.append(suffix);
    return sb.toString();
  }

  @Override
  @Nullable
  public ResourceId unwindowedFilename(ResourceId outputDirectory, Context context,
      String extension) {
    String filename =
        constructName(
            prefix.get(), shardTemplate, suffix, context.getShardNumber(), context.getNumShards())
        + extension;
    return outputDirectory.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
  }

  @Override
  public ResourceId windowedFilename(ResourceId outputDirectory,
      WindowedContext c, String extension) {
    throw new UnsupportedOperationException("There is no default policy for windowed file"
        + " output. Please provide an explicit FilenamePolicy to generate filenames.");
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    String filenamePattern;
    if (prefix.isAccessible()) {
      filenamePattern = String.format("%s%s%s", prefix.get(), shardTemplate, suffix);
    } else {
      filenamePattern = String.format("%s%s%s", prefix, shardTemplate, suffix);
    }
    builder.add(
        DisplayData.item("filenamePattern", filenamePattern)
            .withLabel("Filename Pattern"));
  }

  private static class ExtractFilename implements SerializableFunction<ResourceId, String> {
    @Override
    public String apply(ResourceId input) {
      if (input.isDirectory()) {
        return "";
      } else {
        return firstNonNull(input.getFilename(), "");
      }
    }
  }
}
