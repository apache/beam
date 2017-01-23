/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat;

import static org.junit.Assert.assertEquals;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.Read;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOContants;
import org.apache.beam.sdk.io.hadoop.inputformat.coders.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.Row;

@RunWith(JUnit4.class)
public class HadoopInputFormatCoderTests {

  @Test
  public void testMapWritableEncoding() throws Exception {
    MapWritable map = new MapWritable();
    map.put(new Text("path"), new Text("/home/asharma/MOCK1.csv"));
    map.put(new Text("country"), new Text("Czech Republic"));
    map.put(new Text("@timestamp"), new Text("2016-11-11T08:06:42.260Z"));
    map.put(new Text("gender"), new Text("Male"));
    map.put(new Text("@version"), new Text("1"));
    map.put(new Text("Id"), new Text("131"));
    map.put(new Text("salary"), new Text("$8.65"));
    map.put(new Text("email"), new Text("alex@example.com"));
    map.put(new Text("desc"), new Text("Other contact with macaw, subsequent encounter"));
    WritableCoder<MapWritable> coder = WritableCoder.of(MapWritable.class);
    CoderUtils.clone(coder, map);
    CoderProperties.coderDecodeEncodeEqual(coder, map);
  }

  @Test
  public void testDefaultCoderFromCodeRegistry() {
    TypeDescriptor<Long> td = new TypeDescriptor<Long>() {};
    Configuration conf = loadTestConfiguration();
    Pipeline pipeline = TestPipeline.create();
    Read<Text, String> read = HadoopInputFormatIO.<Text, String>read().withConfiguration(conf);
    Coder<Long> coder = read.getDefaultCoder(td, pipeline.getCoderRegistry());
    assertEquals(coder.getClass(), VarLongCoder.class);
  }

  @Test
  public void testWritableCoder() {
    TypeDescriptor<MapWritable> td = new TypeDescriptor<MapWritable>() {};
    Configuration conf = loadTestConfiguration();
    DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
    Pipeline pipeline = Pipeline.create(directRunnerOptions);
    Read<Text, String> read = HadoopInputFormatIO.<Text, String>read().withConfiguration(conf);
    Coder<MapWritable> coder = read.getDefaultCoder(td, pipeline.getCoderRegistry());
    assertEquals(coder.getClass(), WritableCoder.class);
  }

  @Test(expected = IllegalStateException.class)
  public void testNonRegisteredCustomCoder() {
    TypeDescriptor<Row> td = new TypeDescriptor<Row>() {};
    Configuration conf = loadTestConfiguration();
    DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
    Pipeline pipeline = Pipeline.create(directRunnerOptions);
    Read<Text, String> read = HadoopInputFormatIO.<Text, String>read().withConfiguration(conf);
    read.getDefaultCoder(td, pipeline.getCoderRegistry());
  }

  private Configuration loadTestConfiguration() {
    Configuration conf = new Configuration();
    conf.set(ConfigurationOptions.ES_NODES, "10.51.234.135:9200");
    conf.set("es.resource", "/my_data/logs");
    conf.setClass(HadoopInputFormatIOContants.INPUTFORMAT_CLASSNAME,
        org.elasticsearch.hadoop.mr.EsInputFormat.class, InputFormat.class);
    conf.setClass(HadoopInputFormatIOContants.KEY_CLASS, Text.class, Object.class);
    conf.setClass(HadoopInputFormatIOContants.VALUE_CLASS, MapWritable.class, Object.class);
    return conf;
  }
}
