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
package org.apache.beam.sdk.io.hdfs;

import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for HDFSFileSource.
 */
public class HDFSFileSourceTest {

  private Random random = new Random(0L);

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testFullyReadSingleFile() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<KV<IntWritable, Text>> expectedResults = createRandomRecords(3, 10, 0);
    File file = createFileWithData("tmp.seq", expectedResults);

    HDFSFileSource<KV<IntWritable, Text>, IntWritable, Text> source =
            HDFSFileSource.from(
                    file.toString(), SequenceFileInputFormat.class, IntWritable.class, Text.class);

    assertEquals(file.length(), source.getEstimatedSizeBytes(null));

    assertThat(expectedResults, containsInAnyOrder(readFromSource(source, options).toArray()));
  }

  @Test
  public void testFullyReadSingleFileWithSpaces() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<KV<IntWritable, Text>> expectedResults = createRandomRecords(3, 10, 0);
    File file = createFileWithData("tmp data.seq", expectedResults);

    HDFSFileSource<KV<IntWritable, Text>, IntWritable, Text> source =
            HDFSFileSource.from(
                    file.toString(), SequenceFileInputFormat.class, IntWritable.class, Text.class);

    assertEquals(file.length(), source.getEstimatedSizeBytes(null));

    assertThat(expectedResults, containsInAnyOrder(readFromSource(source, options).toArray()));
  }

  @Test
  public void testFullyReadFilePattern() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<KV<IntWritable, Text>> data1 = createRandomRecords(3, 10, 0);
    File file1 = createFileWithData("file1", data1);

    List<KV<IntWritable, Text>> data2 = createRandomRecords(3, 10, 10);
    createFileWithData("file2", data2);

    List<KV<IntWritable, Text>> data3 = createRandomRecords(3, 10, 20);
    createFileWithData("file3", data3);

    List<KV<IntWritable, Text>> data4 = createRandomRecords(3, 10, 30);
    createFileWithData("otherfile", data4);

    List<KV<IntWritable, Text>> expectedResults = new ArrayList<>();
    expectedResults.addAll(data1);
    expectedResults.addAll(data2);
    expectedResults.addAll(data3);

    HDFSFileSource<KV<IntWritable, Text>, IntWritable, Text> source =
        HDFSFileSource.from(
            new File(file1.getParent(), "file*").toString(), SequenceFileInputFormat.class,
            IntWritable.class, Text.class);

    assertThat(expectedResults, containsInAnyOrder(readFromSource(source, options).toArray()));
  }

  @Test
  public void testCloseUnstartedFilePatternReader() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<KV<IntWritable, Text>> data1 = createRandomRecords(3, 10, 0);
    File file1 = createFileWithData("file1", data1);

    List<KV<IntWritable, Text>> data2 = createRandomRecords(3, 10, 10);
    createFileWithData("file2", data2);

    List<KV<IntWritable, Text>> data3 = createRandomRecords(3, 10, 20);
    createFileWithData("file3", data3);

    List<KV<IntWritable, Text>> data4 = createRandomRecords(3, 10, 30);
    createFileWithData("otherfile", data4);

    HDFSFileSource<KV<IntWritable, Text>, IntWritable, Text> source =
        HDFSFileSource.from(
            new File(file1.getParent(), "file*").toString(),
            SequenceFileInputFormat.class, IntWritable.class, Text.class);
    Source.Reader<KV<IntWritable, Text>> reader = source.createReader(options);

    // Closing an unstarted FilePatternReader should not throw an exception.
    try {
      reader.close();
    } catch (Exception e) {
      fail("Closing an unstarted FilePatternReader should not throw an exception");
    }
  }

  @Test
  public void testSplits() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();

    List<KV<IntWritable, Text>> expectedResults = createRandomRecords(3, 10000, 0);
    File file = createFileWithData("tmp.seq", expectedResults);

    HDFSFileSource<KV<IntWritable, Text>, IntWritable, Text> source =
        HDFSFileSource.from(
            file.toString(), SequenceFileInputFormat.class, IntWritable.class, Text.class);

    // Assert that the source produces the expected records
    assertEquals(expectedResults, readFromSource(source, options));

    // Split with a small bundle size (has to be at least size of sync interval)
    List<? extends BoundedSource<KV<IntWritable, Text>>> splits = source
        .split(SequenceFile.SYNC_INTERVAL, options);
    assertTrue(splits.size() > 2);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splits, options);
    int nonEmptySplits = 0;
    for (BoundedSource<KV<IntWritable, Text>> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    assertTrue(nonEmptySplits > 2);
  }

  @Test
  public void testSplitEstimatedSize() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();

    List<KV<IntWritable, Text>> expectedResults = createRandomRecords(3, 10000, 0);
    File file = createFileWithData("tmp.avro", expectedResults);

    HDFSFileSource<KV<IntWritable, Text>, IntWritable, Text> source =
        HDFSFileSource.from(file.toString(), SequenceFileInputFormat.class,
            IntWritable.class, Text.class);

    long originalSize = source.getEstimatedSizeBytes(options);
    long splitTotalSize = 0;
    List<? extends BoundedSource<KV<IntWritable, Text>>> splits = source.split(
        SequenceFile.SYNC_INTERVAL, options
    );
    for (BoundedSource<KV<IntWritable, Text>> splitSource : splits) {
      splitTotalSize += splitSource.getEstimatedSizeBytes(options);
    }
    // Assert that the estimated size of the whole is the sum of its parts
    assertEquals(originalSize, splitTotalSize);
  }

  private File createFileWithData(String filename, List<KV<IntWritable, Text>> records)
      throws IOException {
    File tmpFile = tmpFolder.newFile(filename);
    try (Writer writer = SequenceFile.createWriter(new Configuration(),
        Writer.keyClass(IntWritable.class), Writer.valueClass(Text.class),
        Writer.file(new Path(tmpFile.toURI())))) {

      for (KV<IntWritable, Text> record : records) {
        writer.append(record.getKey(), record.getValue());
      }
    }
    return tmpFile;
  }

  private List<KV<IntWritable, Text>> createRandomRecords(int dataItemLength,
                                                          int numItems, int offset) {
    List<KV<IntWritable, Text>> records = new ArrayList<>();
    for (int i = 0; i < numItems; i++) {
      IntWritable key = new IntWritable(i + offset);
      Text value = new Text(createRandomString(dataItemLength));
      records.add(KV.of(key, value));
    }
    return records;
  }

  private String createRandomString(int length) {
    char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < length; i++) {
      builder.append(chars[random.nextInt(chars.length)]);
    }
    return builder.toString();
  }

}
