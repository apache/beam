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

/**
 * Standard shard naming templates.
 *
 * <p>Shard naming templates are strings that may contain placeholders for the shard number and
 * shard count. When constructing a filename for a particular shard number, the upper-case letters
 * 'S' and 'N' are replaced with the 0-padded shard number and shard count respectively.
 *
 * <p>Left-padding of the numbers enables lexicographical sorting of the resulting filenames. If the
 * shard number or count are too large for the space provided in the template, then the result may
 * no longer sort lexicographically. For example, a shard template of "S-of-N", for 200 shards, will
 * result in outputs named "0-of-200", ... '10-of-200', '100-of-200", etc.
 *
 * <p>Shard numbers start with 0, so the last shard number is the shard count minus one. For
 * example, the template "-SSSSS-of-NNNNN" will be instantiated as "-00000-of-01000" for the first
 * shard (shard 0) of a 1000-way sharded output.
 *
 * <p>A shard name template is typically provided along with a name prefix and suffix, which allows
 * constructing complex paths that have embedded shard information. For example, outputs in the form
 * "gs://bucket/path-01-of-99.txt" could be constructed by providing the individual components:
 *
 * <pre>{@code
 * pipeline.apply(
 *     TextIO.write().to("gs://bucket/path")
 *                 .withShardNameTemplate("-SS-of-NN")
 *                 .withSuffix(".txt"))
 * }</pre>
 *
 * <p>In the example above, you could make parts of the output configurable by users without the
 * user having to specify all components of the output name.
 *
 * <p>If a shard name template does not contain any repeating 'S', then the output shard count must
 * be 1, as otherwise the same filename would be generated for multiple shards.
 */
public class ShardNameTemplate {
  /**
   * Shard name containing the index and max.
   *
   * <p>Eg: [prefix]-00000-of-00100[suffix] and [prefix]-00001-of-00100[suffix]
   */
  public static final String INDEX_OF_MAX = "-SSSSS-of-NNNNN";

  /**
   * Shard is a file within a directory.
   *
   * <p>Eg: [prefix]/part-00000[suffix] and [prefix]/part-00001[suffix]
   */
  public static final String DIRECTORY_CONTAINER = "/part-SSSSS";
}
