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
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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
  public void testGeneric() throws Exception {
    populateFile();

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    @SuppressWarnings("unchecked")
    Class<? extends FileInputFormat<IntWritable, Text>> inputFormatClass =
        (Class<? extends FileInputFormat<IntWritable, Text>>) (Class<?>) SequenceFileInputFormat.class;
    HadoopIO.Read.Bound<IntWritable,Text> bound =
        HadoopIO.Read.from(inputFile.getAbsolutePath(), inputFormatClass, IntWritable.class, Text.class);
    PCollection<KV<IntWritable, Text>> input = p.apply(bound);
    input.apply(ParDo.of(new TabSeparatedString()))
        .apply(TextIO.Write.to(outputFile.getAbsolutePath()).withoutSharding());
    EvaluationResult res = SparkPipelineRunner.create().run(p);
    res.close();

    List<String> records = Files.readLines(outputFile, Charsets.UTF_8);
    for (int i = 0; i < 5; i++) {
      assertEquals(i + "\tvalue-" + i, records.get(i));
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

  static class TabSeparatedString extends DoFn<KV<IntWritable, Text>, String> {
    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element().getKey().toString() + "\t" + c.element().getValue().toString());
    }
  }

}
