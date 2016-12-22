package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;

import java.io.IOException;
import java.io.Serializable;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.coders.CassandraRowCoder;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.MyCassandraRow;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.Test;

import com.datastax.driver.core.Row;

public class HIFWithCassandraTest implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Test
	public void testHadoopInputFormatSource() throws IOException {

		DirectOptions directRunnerOptions = PipelineOptionsFactory
				.as(DirectOptions.class);
		Pipeline p = Pipeline.create(directRunnerOptions);
		Configuration conf = new Configuration();

		String KEYSPACE = "mobile_data_usage";

		String COLUMN_FAMILY = "subscriber";

		conf.set("cassandra.input.thrift.port", "9160");

		conf.set("cassandra.input.thrift.address", "127.0.0.1");// 10.51.234.135

		conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");

		conf.set("cassandra.input.keyspace", KEYSPACE);

		conf.set("cassandra.input.columnfamily", COLUMN_FAMILY);

		Class<?> inputFormatClassName;
		try {
			inputFormatClassName = Class
					.forName("org.apache.cassandra.hadoop.cql3.CqlInputFormat");

			conf.setClass("mapreduce.job.inputformat.class",
					inputFormatClassName, InputFormat.class);

			Class<?> keyClass = Class.forName("java.lang.Long");

			conf.setClass("key.class", keyClass, Object.class);

			Class<?> valueClass = Class.forName("com.datastax.driver.core.Row");

			conf.setClass("value.class", valueClass, Object.class);

			SimpleFunction<Row, MyCassandraRow> myValueTranslate = new SimpleFunction<Row, MyCassandraRow>() {

				@Override
				public MyCassandraRow apply(Row input) {
					return new MyCassandraRow(
							input.getString("subscriber_email")); // no idea if
																	// getColumn
					//  is a real
					// function
				}
			};
			p.getCoderRegistry().registerCoder(MyCassandraRow.class,
					CassandraRowCoder.class);
			PCollection<?> cassandraData = p.apply(HadoopInputFormatIO
					.<KV<Long, MyCassandraRow>> read().withConfiguration(conf)
					.withValueTranslation(myValueTranslate));
			/*
			 * PCollection<Long> apply = cassandraData.apply(Count.<KV<Long,
			 * MyCassandraRow>>globally());
			 * PAssert.thatSingleton(apply).isEqualTo((long) 3);
			 */
			p.run().waitUntilFinish();
		} catch (ClassNotFoundException e) {
			// Need to handle
		}
	}
}
