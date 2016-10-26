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

package org.apache.beam.runners.spark.io.hadoop;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;

/**
 * Shard name builder.
 */
public final class ShardNameBuilder {

  private ShardNameBuilder() {
  }

  /**
   * Replace occurrences of uppercase letters 'N' with the given {code}shardCount{code},
   * left-padded with zeros if necessary.
   * @see org.apache.beam.sdk.io.ShardNameTemplate
   * @param template the string template containing uppercase letters 'N'
   * @param shardCount the total number of shards
   * @return a string template with 'N' replaced by the shard count
   */
  public static String replaceShardCount(String template, int shardCount) {
    return replaceShardPattern(template, "N+", shardCount);
  }

  /**
   * Replace occurrences of uppercase letters 'S' with the given {code}shardNumber{code},
   * left-padded with zeros if necessary.
   * @see org.apache.beam.sdk.io.ShardNameTemplate
   * @param template the string template containing uppercase letters 'S'
   * @param shardNumber the number of a particular shard
   * @return a string template with 'S' replaced by the shard number
   */
  public static String replaceShardNumber(String template, int shardNumber) {
    return replaceShardPattern(template, "S+", shardNumber);
  }

  private static String replaceShardPattern(String template, String pattern, int n) {
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(template);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      // replace pattern with a String format string:
      // index 1, zero-padding flag (0), width length of matched pattern, decimal conversion
      m.appendReplacement(sb, "%1\\$0" + m.group().length() + "d");
    }
    m.appendTail(sb);
    return String.format(sb.toString(), n);
  }

  /**
   * @param pathPrefix a relative or absolute path
   * @param template a template string
   * @return the output directory for the given prefix, template and suffix
   */
  public static String getOutputDirectory(String pathPrefix, String template) {
    String out = new Path(pathPrefix + template).getParent().toString();
    if (out.isEmpty()) {
      return "./";
    }
    return out;
  }

  /**
   * @param pathPrefix a relative or absolute path
   * @param template a template string
   * @return the prefix of the output filename for the given path prefix and template
   */
  public static String getOutputFilePrefix(String pathPrefix, String template) {
    String name = new Path(pathPrefix + template).getName();
    if (name.endsWith(template)) {
      return name.substring(0, name.length() - template.length());
    } else {
      return "";
    }
  }

  /**
   * @param pathPrefix a relative or absolute path
   * @param template a template string
   * @return the template for the output filename for the given path prefix and
   * template
   */
  public static String getOutputFileTemplate(String pathPrefix, String template) {
    String name = new Path(pathPrefix + template).getName();
    if (name.endsWith(template)) {
      return template;
    } else {
      return name;
    }
  }
}
