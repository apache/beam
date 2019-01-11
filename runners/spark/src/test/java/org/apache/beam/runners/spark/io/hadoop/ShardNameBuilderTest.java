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

import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.getOutputDirectory;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.getOutputFilePrefix;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.getOutputFileTemplate;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.replaceShardCount;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.replaceShardNumber;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Test on the {@link ShardNameBuilder}.
 */
public class ShardNameBuilderTest {

  @Test
  public void testReplaceShardCount() {
    assertEquals("", replaceShardCount("", 6));
    assertEquals("-S-of-6", replaceShardCount("-S-of-N", 6));
    assertEquals("-SS-of-06", replaceShardCount("-SS-of-NN", 6));
    assertEquals("-S-of-60", replaceShardCount("-S-of-N", 60));
    assertEquals("-SS-of-60", replaceShardCount("-SS-of-NN", 60));
    assertEquals("/part-SSSSS", replaceShardCount("/part-SSSSS", 6));
  }

  @Test
  public void testReplaceShardNumber() {
    assertEquals("", replaceShardNumber("", 5));
    assertEquals("-5-of-6", replaceShardNumber("-S-of-6", 5));
    assertEquals("-05-of-06", replaceShardNumber("-SS-of-06", 5));
    assertEquals("-59-of-60", replaceShardNumber("-S-of-60", 59));
    assertEquals("-59-of-60", replaceShardNumber("-SS-of-60", 59));
    assertEquals("/part-00005", replaceShardNumber("/part-SSSSS", 5));
  }

  @Test
     public void testGetOutputDirectory() {
    assertEquals("./", getOutputDirectory("foo", "-S-of-N"));
    assertEquals("foo", getOutputDirectory("foo/bar", "-S-of-N"));
    assertEquals("/foo", getOutputDirectory("/foo/bar", "-S-of-N"));
    assertEquals("hdfs://foo/", getOutputDirectory("hdfs://foo/bar", "-S-of-N"));
    assertEquals("foo/bar", getOutputDirectory("foo/bar", "/part-SSSSS"));
    assertEquals("/foo/bar", getOutputDirectory("/foo/bar", "/part-SSSSS"));
    assertEquals("hdfs://foo/bar", getOutputDirectory("hdfs://foo/bar", "/part-SSSSS"));
  }

  @Test
  public void testGetOutputFilePrefix() {
    assertEquals("foo", getOutputFilePrefix("foo", "-S-of-N"));
    assertEquals("bar", getOutputFilePrefix("foo/bar", "-S-of-N"));
    assertEquals("bar", getOutputFilePrefix("/foo/bar", "-S-of-N"));
    assertEquals("bar", getOutputFilePrefix("hdfs://foo/bar", "-S-of-N"));
    assertEquals("", getOutputFilePrefix("foo/bar", "/part-SSSSS"));
    assertEquals("", getOutputFilePrefix("/foo/bar", "/part-SSSSS"));
    assertEquals("", getOutputFilePrefix("hdfs://foo/bar", "/part-SSSSS"));
  }

  @Test
  public void testGetOutputFileTemplate() {
    assertEquals("-S-of-N", getOutputFileTemplate("foo", "-S-of-N"));
    assertEquals("-S-of-N", getOutputFileTemplate("foo/bar", "-S-of-N"));
    assertEquals("-S-of-N", getOutputFileTemplate("/foo/bar", "-S-of-N"));
    assertEquals("-S-of-N", getOutputFileTemplate("hdfs://foo/bar", "-S-of-N"));
    assertEquals("part-SSSSS", getOutputFileTemplate("foo/bar", "/part-SSSSS"));
    assertEquals("part-SSSSS", getOutputFileTemplate("/foo/bar", "/part-SSSSS"));
    assertEquals("part-SSSSS", getOutputFileTemplate("hdfs://foo/bar", "/part-SSSSS"));
  }

}
