package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;

import java.io.IOException;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class HIFWithTextTest {

	@Test
	public void testHadoopInputFormatSource() throws IOException {
		// PipelineOptions options = PipelineOptionsFactory.create();
		// Pipeline p = Pipeline.create(options);
		// TestPipeline p = TestPipeline.create();
		DirectOptions directRunnerOptions = PipelineOptionsFactory
				.as(DirectOptions.class);
		Pipeline p = Pipeline.create(directRunnerOptions);
		Configuration conf = new Configuration();

		Class<?> inputFormatClassName;
		conf.set("mapred.input.dir", StringUtils.escapeString("D:\\2.txt"));
		conf.set("mapred.max.split.size", "500");
		Class keyClass = org.apache.hadoop.io.Text.class;
		conf.setClass("key.class", keyClass, Object.class);
		Class valueClass = org.apache.hadoop.io.Text.class;
		conf.setClass("value.class", valueClass, Object.class);

		// p.getCoderRegistry().registerCoder(MyCassandraRow.class,
		// CassandraRowCoder.class);
		PCollection<?> data = p.apply(HadoopInputFormatIO
				.<KV<Text, Text>> read().withConfiguration(conf));

		p.run().waitUntilFinish();
		System.out.println("Output = " + data.toString());
	}
}
