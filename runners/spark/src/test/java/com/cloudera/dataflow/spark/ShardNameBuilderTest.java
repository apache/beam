/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import org.junit.Test;

import static com.cloudera.dataflow.spark.ShardNameBuilder.getOutputDirectory;
import static com.cloudera.dataflow.spark.ShardNameBuilder.getOutputFile;
import static com.cloudera.dataflow.spark.ShardNameBuilder.replaceShardCount;
import static com.cloudera.dataflow.spark.ShardNameBuilder.replaceShardNumber;
import static org.junit.Assert.assertEquals;

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
    assertEquals("foo", getOutputDirectory("foo/bar", "-S-of-N", ""));
    assertEquals("/foo", getOutputDirectory("/foo/bar", "-S-of-N", ""));
    assertEquals("hdfs://foo/", getOutputDirectory("hdfs://foo/bar", "-S-of-N", ".txt"));
    assertEquals("foo/bar", getOutputDirectory("foo/bar", "/part-SSSSS", ""));
    assertEquals("/foo/bar", getOutputDirectory("/foo/bar", "/part-SSSSS", ""));
    assertEquals("hdfs://foo/bar", getOutputDirectory("hdfs://foo/bar", "/part-SSSSS", ".txt"));
  }

  @Test
  public void testGetOutputFile() {
    assertEquals("bar-S-of-N", getOutputFile("foo/bar", "-S-of-N", ""));
    assertEquals("bar-S-of-N", getOutputFile("/foo/bar", "-S-of-N", ""));
    assertEquals("bar-S-of-N.txt", getOutputFile("hdfs://foo/bar", "-S-of-N", ".txt"));
    assertEquals("part-SSSSS", getOutputFile("foo/bar", "/part-SSSSS", ""));
    assertEquals("part-SSSSS", getOutputFile("/foo/bar", "/part-SSSSS", ""));
    assertEquals("part-SSSSS.txt", getOutputFile("hdfs://foo/bar", "/part-SSSSS", ".txt"));
  }

}
