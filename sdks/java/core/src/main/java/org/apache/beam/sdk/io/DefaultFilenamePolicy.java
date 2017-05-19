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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default {@link FilenamePolicy} for windowed and unwindowed files. This policy is constructed
 * using three parameters that together define the output name of a sharded file, in conjunction
 * with the number of shards, index of the particular file, current window and pane information,
 * using {@link #constructName}.
 *
 * <p>Most users will use this {@link DefaultFilenamePolicy}. For more advanced
 * uses in generating different files for each window and other sharding controls, see the
 * {@code WriteOneFilePerWindow} example pipeline.
 */
public final class DefaultFilenamePolicy extends FilenamePolicy {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultFilenamePolicy.class);

  /** The default sharding name template used in {@link #constructUsingStandardParameters}. */
  public static final String DEFAULT_SHARD_TEMPLATE = ShardNameTemplate.INDEX_OF_MAX;

  /** The default windowed sharding name template used when writing windowed files.
   *  Currently this is automatically appended to provided sharding name template
   *  when there is a need to write windowed files.
   */
  private static final String DEFAULT_WINDOWED_SHARED_TEMPLATE_SUFFIX =
      "-PPP-firstpane-F-lastpane-L-W";

  /*
   * pattern for only non-windowed file names
   */
  private static final String NON_WINDOWED_ONLY_PATTERN = "S+|N+";

  /*
   * pattern for only windowed file names
   */
  private static final String WINDOWED_ONLY_PATTERN = "P+|L|F|W";

  /*
   * pattern for both windowed and non-windowed file names
   */
  private static final String TEMPLATE_PATTERN = "(" + NON_WINDOWED_ONLY_PATTERN + "|"
   + WINDOWED_ONLY_PATTERN + ")";

  // Pattern that matches shard placeholders within a shard template.
  private static final Pattern SHARD_FORMAT_RE = Pattern.compile(TEMPLATE_PATTERN);
  private static final Pattern WINDOWED_FORMAT_RE = Pattern.compile(WINDOWED_ONLY_PATTERN);

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
    boolean isWindowed = isWindowedTemplate(this.shardTemplate);
    if (!isWindowed){
      LOG.info("Template {} does not have enough information to create windowed file names."
          + "Will use default template for windowed file names if needed.", this.shardTemplate);
    }
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
   * <p>Shard name template will automatically be expanded in case when there is
   * need to write windowed files and user did not provide enough information in the
   * template to deal with windowed file names.
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

  /*
   * Checks whether given template contains enough information to form windowed file names
   */
  static boolean isWindowedTemplate(String template){
    if (template != null){
      Matcher m = WINDOWED_FORMAT_RE.matcher(template);
      return m.find();
    }
    return false;
  }

  /**
   * Constructs a fully qualified name from components.
   *
   * <p>The name is built from a prefix, shard template (with shard numbers
   * applied), and a suffix.  All components are required, but may be empty
   * strings.
   *
   * <p>Within a shard template, repeating sequences of the letters "S" or "N"
   * are replaced with the shard number, or number of shards respectively.
   * Repeating sequence of "P" is replaced with the index of the window pane.
   * "L" is replaced with "true" in case of last pane or "false" otherwise.
   * "F" is replaced with "true" in case of first pane or "false" otherwise.
   * "W" is replaced by stringification of current window.
   *
   * <p>The numbers are formatted with leading zeros to match the length of the
   * repeated sequence of letters.
   *
   * <p>For example, if prefix = "output", shardTemplate = "-SSS-of-NNN", and
   * suffix = ".txt", with shardNum = 1 and numShards = 100, the following is
   * produced:  "output-001-of-100.txt".
   */
  static String constructName(
      String prefix, String shardTemplate, String suffix, int shardNum, int numShards,
      long currentPaneIndex, boolean firstPane, boolean lastPane, String windowStr) {
    // Matcher API works with StringBuffer, rather than StringBuilder.
    StringBuffer sb = new StringBuffer();
    sb.append(prefix);

    Matcher m = SHARD_FORMAT_RE.matcher(shardTemplate);
    while (m.find()) {
      boolean isCurrentShardNum = (m.group(1).charAt(0) == 'S');
      boolean isNumberOfShards = (m.group(1).charAt(0) == 'N');
      boolean isCurrentPaneIndex = (m.group(1).charAt(0) == 'P') && currentPaneIndex > -1;
      boolean isWindow = (m.group(1).charAt(0) == 'W') && windowStr != null;
      boolean isLastPane = (m.group(1).charAt(0) == 'L');
      boolean isFirstPane = (m.group(1).charAt(0) == 'F');

      char[] zeros = new char[m.end() - m.start()];
      Arrays.fill(zeros, '0');
      DecimalFormat df = new DecimalFormat(String.valueOf(zeros));
      if (isCurrentShardNum) {
        String formatted = df.format(shardNum);
        m.appendReplacement(sb, formatted);
      } else if (isNumberOfShards) {
        String formatted = df.format(numShards);
        m.appendReplacement(sb, formatted);
      } else if (isCurrentPaneIndex) {
        String formatted = df.format(currentPaneIndex);
        m.appendReplacement(sb, formatted);
      } else if (isWindow) {
        m.appendReplacement(sb, windowStr);
      } else if (isFirstPane){
        m.appendReplacement(sb, String.valueOf(firstPane));
      } else if (isLastPane){
        m.appendReplacement(sb, String.valueOf(lastPane));
      }
    }
    m.appendTail(sb);

    sb.append(suffix);
    return sb.toString();
  }

  static String constructName(String prefix, String shardTemplate, String suffix, int shardNum,
      int numShards) {
    return constructName(prefix, shardTemplate, suffix, shardNum, numShards, -1L, false,
        false, null);
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
      WindowedContext context, String extension) {

    final PaneInfo paneInfo = context.getPaneInfo();
    long currentPaneIndex = (paneInfo == null ? -1L
        : paneInfo.getIndex());
    boolean firstPane = (paneInfo == null ? false : paneInfo.isFirst());
    boolean lastPane = (paneInfo == null ? false : paneInfo.isLast());
    String windowStr = windowToString(context.getWindow());

    String templateToUse = shardTemplate;
    if (!isWindowedTemplate(shardTemplate)){
      templateToUse = shardTemplate + DEFAULT_WINDOWED_SHARED_TEMPLATE_SUFFIX;
    }

    String filename = constructName(prefix.get(), templateToUse, suffix,
        context.getShardNumber(),
        context.getNumShards(), currentPaneIndex, firstPane, lastPane, windowStr)
        + extension;
    return outputDirectory.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
  }

  /*
   * Since not all windows have toString() that is nice or is compatible to be a part of file name.
   */
  private String windowToString(BoundedWindow window) {
    if (window instanceof GlobalWindow) {
      return "GlobalWindow";
    }
    if (window instanceof IntervalWindow) {
      IntervalWindow iw = (IntervalWindow) window;
      return String.format("IntervalWindow-from-%s-to-%s", iw.start().toString(),
          iw.end().toString());
    }
    return window.toString();
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
