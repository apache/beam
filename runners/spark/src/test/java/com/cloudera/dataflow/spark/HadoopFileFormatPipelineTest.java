/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

import com.cloudera.dataflow.hadoop.HadoopIO;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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

import static org.junit.Assert.assertEquals;

public class HadoopFileFormatPipelineTest {

  private File inputFile;
  private File outputFile;

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

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    @SuppressWarnings("unchecked")
    Class<? extends FileInputFormat<IntWritable, Text>> inputFormatClass =
        (Class<? extends FileInputFormat<IntWritable, Text>>) (Class<?>) SequenceFileInputFormat.class;
    HadoopIO.Read.Bound<IntWritable,Text> read =
        HadoopIO.Read.from(inputFile.getAbsolutePath(), inputFormatClass, IntWritable.class, Text.class);
    PCollection<KV<IntWritable, Text>> input = p.apply(read);
    @SuppressWarnings("unchecked")
    Class<? extends FileOutputFormat<IntWritable, Text>> outputFormatClass =
        (Class<? extends FileOutputFormat<IntWritable, Text>>) (Class<?>) TemplatedSequenceFileOutputFormat.class;
    @SuppressWarnings("unchecked")
    HadoopIO.Write.Bound<IntWritable,Text> write = HadoopIO.Write.to(outputFile.getAbsolutePath(),
        outputFormatClass, IntWritable.class, Text.class);
    input.apply(write.withoutSharding());
    EvaluationResult res = SparkPipelineRunner.create().run(p);
    res.close();

    IntWritable key = new IntWritable();
    Text value = new Text();
    Reader reader = null;
    try {
      reader = new Reader(new Configuration(), Reader.file(new Path(outputFile.toURI())));
      int i = 0;
      while(reader.next(key, value)) {
        assertEquals(i, key.get());
        assertEquals("value-" + i, value.toString());
        i++;
      }
    } finally {
      IOUtils.closeStream(reader);
    }
  }

  private void populateFile() throws IOException {
    IntWritable key = new IntWritable();
    Text value = new Text();
    Writer writer = null;
    try {
      writer = SequenceFile.createWriter(new Configuration(),
          Writer.keyClass(IntWritable.class), Writer.valueClass(Text.class),
          Writer.file(new Path(this.inputFile.toURI())));
      for (int i = 0; i < 5; i++) {
        key.set(i);
        value.set("value-" + i);
        writer.append(key, value);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }

}
