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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import org.apache.beam.runners.spark.PipelineRule;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Pipeline on the Hadoop file format test.
 */
public class HadoopFileFormatPipelineTest {

  private File inputFile;
  private File outputFile;

  @Rule
  public final PipelineRule pipelineRule = PipelineRule.batch();

  @Rule
  public final TemporaryFolder tmpDir = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    inputFile = tmpDir.newFile("test.seq");
    outputFile = tmpDir.newFolder("out");
    outputFile.delete();
  }

  @Test
  public void testSequenceFile() throws Exception {
    populateFile();

    Pipeline p = pipelineRule.createPipeline();
    @SuppressWarnings("unchecked")
    Class<? extends FileInputFormat<IntWritable, Text>> inputFormatClass =
        (Class<? extends FileInputFormat<IntWritable, Text>>)
            (Class<?>) SequenceFileInputFormat.class;
    HadoopIO.Read.Bound<IntWritable, Text> read =
        HadoopIO.Read.from(inputFile.getAbsolutePath(),
            inputFormatClass,
            IntWritable.class,
            Text.class);
    PCollection<KV<IntWritable, Text>> input = p.apply(read)
        .setCoder(KvCoder.of(WritableCoder.of(IntWritable.class), WritableCoder.of(Text.class)));
    @SuppressWarnings("unchecked")
    Class<? extends FileOutputFormat<IntWritable, Text>> outputFormatClass =
        (Class<? extends FileOutputFormat<IntWritable, Text>>)
            (Class<?>) TemplatedSequenceFileOutputFormat.class;
    @SuppressWarnings("unchecked")
    HadoopIO.Write.Bound<IntWritable, Text> write = HadoopIO.Write.to(outputFile.getAbsolutePath(),
        outputFormatClass, IntWritable.class, Text.class);
    input.apply(write.withoutSharding());
    p.run().waitUntilFinish();

    IntWritable key = new IntWritable();
    Text value = new Text();
    try (Reader reader = new Reader(new Configuration(),
        Reader.file(new Path(outputFile.toURI())))) {
      int i = 0;
      while (reader.next(key, value)) {
        assertEquals(i, key.get());
        assertEquals("value-" + i, value.toString());
        i++;
      }
    }
  }

  private void populateFile() throws IOException {
    IntWritable key = new IntWritable();
    Text value = new Text();
    try (Writer writer = SequenceFile.createWriter(
        new Configuration(),
        Writer.keyClass(IntWritable.class), Writer.valueClass(Text.class),
        Writer.file(new Path(this.inputFile.toURI())))) {
      for (int i = 0; i < 5; i++) {
        key.set(i);
        value.set("value-" + i);
        writer.append(key, value);
      }
    }
  }

}
