package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests;

import static org.junit.Assert.assertEquals;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.Read;
import org.apache.beam.sdk.io.hadoop.inputformat.coders.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.junit.Test;

import com.datastax.driver.core.Row;

public class HadoopInputFormatCoderTests {

	@Test
	public void testMapWritableEncoding() throws Exception {
		MapWritable map = new LinkedMapWritable();
		map.put(new Text("path"), new Text("/home/asharma/MOCK1.csv"));
		map.put(new Text("country"), new Text("Czech Republic"));
		map.put(new Text("@timestamp"), new Text("2016-11-11T08:06:42.260Z"));
		map.put(new Text("gender"), new Text("Male"));
		map.put(new Text("@version"), new Text("1"));
		map.put(new Text("Id"), new Text("131"));
		map.put(new Text("salary"), new Text("$8.65"));
		map.put(new Text("email"), new Text("anichols3m@fastcompany.com"));
		map.put(new Text("desc"), new Text(
				"Other contact with macaw, subsequent encounter"));
		/*
		 * map.put(new Text("one"), new VIntWritable(1)); map.put(new
		 * Text("two"), new VLongWritable(2));
		 */
		WritableCoder<MapWritable> coder = WritableCoder.of(MapWritable.class);
		CoderUtils.clone(coder, map);
		CoderProperties.coderDecodeEncodeEqual(coder, map);
	}

	@Test
	public void testDefaultCoderFromCodeRegistry() {
		TypeDescriptor<Long> td = new TypeDescriptor<Long>() {
		};
		Configuration conf = loadTestConfiguration();
		DirectOptions directRunnerOptions = PipelineOptionsFactory
				.as(DirectOptions.class);
		Pipeline pipeline = Pipeline.create(directRunnerOptions);
		Read read = HadoopInputFormatIO.<Text, String> read()
				.withConfiguration(conf);
		Coder coder = read.getDefaultCoder(td, pipeline);

		assertEquals(coder.getClass(), VarLongCoder.class);

	}

	@Test
	public void testWritableCoder() {
		TypeDescriptor<MapWritable> td = new TypeDescriptor<MapWritable>() {
		};
		Configuration conf = loadTestConfiguration();
		DirectOptions directRunnerOptions = PipelineOptionsFactory
				.as(DirectOptions.class);
		Pipeline pipeline = Pipeline.create(directRunnerOptions);
		Read read = HadoopInputFormatIO.<Text, String> read()
				.withConfiguration(conf);
		Coder coder = read.getDefaultCoder(td, pipeline);

		assertEquals(coder.getClass(), WritableCoder.class);
	}

	@Test(expected=IllegalStateException.class)
	public void testNonRegisteredCustomCoder() {
		TypeDescriptor<Row> td = new TypeDescriptor<Row>() {
		};
		Configuration conf = loadTestConfiguration();
		DirectOptions directRunnerOptions = PipelineOptionsFactory
				.as(DirectOptions.class);
		Pipeline pipeline = Pipeline.create(directRunnerOptions);

		Read read = HadoopInputFormatIO.<Text, String> read()
				.withConfiguration(conf);
		Coder coder = read.getDefaultCoder(td, pipeline);

	}

	private Configuration loadTestConfiguration() {
		Configuration conf = new Configuration();
		conf.set(ConfigurationOptions.ES_NODES, "10.51.234.135:9200");
		conf.set("es.resource", "/my_data/logs");

		Class inputFormatClassName;
		inputFormatClassName = org.elasticsearch.hadoop.mr.EsInputFormat.class;

		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName,
				InputFormat.class);

		Class keyClass = Text.class;

		conf.setClass("key.class", keyClass, Object.class);

		Class valueClass = MapWritable.class;

		conf.setClass("value.class", valueClass, Object.class);
		return conf;
	}
}
