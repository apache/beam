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

import static org.junit.Assert.assertEquals;

import com.google.common.base.MoreObjects;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for HDFSFileSinkTest.
 */
public class HDFSFileSinkTest {

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private final String part0 = "part-r-00000";
  private final String foobar = "foobar";

  private <T> void doWrite(Sink<T> sink,
                           PipelineOptions options,
                           Iterable<T> toWrite) throws Exception {
    Sink.WriteOperation<T, String> writeOperation =
        (Sink.WriteOperation<T, String>) sink.createWriteOperation();
    Sink.Writer<T, String> writer = writeOperation.createWriter(options);
    writer.openUnwindowed(UUID.randomUUID().toString(),  -1, -1);
    for (T t: toWrite) {
      writer.write(t);
    }
    String writeResult = writer.close();
    writeOperation.finalize(Collections.singletonList(writeResult), options);
  }

  @Test
  public void testWriteSingleRecord() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    File file = tmpFolder.newFolder();

    HDFSFileSink<String, NullWritable, Text> sink =
        HDFSFileSink.to(
            file.toString(),
            SequenceFileOutputFormat.class,
            NullWritable.class,
            Text.class,
            new SerializableFunction<String, KV<NullWritable, Text>>() {
              @Override
              public KV<NullWritable, Text> apply(String input) {
                return KV.of(NullWritable.get(), new Text(input));
              }
            });

    doWrite(sink, options, Collections.singletonList(foobar));

    SequenceFile.Reader.Option opts =
        SequenceFile.Reader.file(new Path(file.toString(), part0));
    SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), opts);
    assertEquals(NullWritable.class.getName(), reader.getKeyClassName());
    assertEquals(Text.class.getName(), reader.getValueClassName());
    NullWritable k = NullWritable.get();
    Text v = new Text();
    assertEquals(true, reader.next(k, v));
    assertEquals(NullWritable.get(), k);
    assertEquals(new Text(foobar), v);
  }

  @Test
  public void testToText() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    File file = tmpFolder.newFolder();

    HDFSFileSink<String, NullWritable, Text> sink = HDFSFileSink.toText(file.toString());

    doWrite(sink, options, Collections.singletonList(foobar));

    List<String> strings = Files.readAllLines(new File(file.toString(), part0).toPath(),
        Charset.forName("UTF-8"));
    assertEquals(Collections.singletonList(foobar), strings);
  }

  @DefaultCoder(AvroCoder.class)
  static class GenericClass {
    int intField;
    String stringField;
    public GenericClass() {}
    public GenericClass(int intValue, String stringValue) {
      this.intField = intValue;
      this.stringField = stringValue;
    }
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("intField", intField)
          .add("stringField", stringField)
          .toString();
    }
    @Override
    public int hashCode() {
      return Objects.hash(intField, stringField);
    }
    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof GenericClass)) {
        return false;
      }
      GenericClass o = (GenericClass) other;
      return Objects.equals(intField, o.intField) && Objects.equals(stringField, o.stringField);
    }
  }

  @Test
  public void testToAvro() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    File file = tmpFolder.newFolder();

    HDFSFileSink<GenericClass, AvroKey<GenericClass>, NullWritable> sink = HDFSFileSink.toAvro(
        file.toString(),
        AvroCoder.of(GenericClass.class),
        new Configuration(false));

    doWrite(sink, options, Collections.singletonList(new GenericClass(3, "foobar")));

    GenericDatumReader datumReader = new GenericDatumReader();
    FileReader<GenericData.Record> reader =
        DataFileReader.openReader(new File(file.getAbsolutePath(), part0 + ".avro"), datumReader);
    GenericData.Record next = reader.next(null);
    assertEquals("foobar", next.get("stringField").toString());
    assertEquals(3, next.get("intField"));
  }

}
