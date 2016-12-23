package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.HadoopInputFormatBoundedSource;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.SerializableConfiguration;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.Test;

import com.datastax.driver.core.Row;

public class HadoopInputFormatIOTest {

	@Test
	public void testReadBuildsCorrectly() {
		Configuration conf = new Configuration();
		String KEYSPACE = "mobile_data_usage";
		String COLUMN_FAMILY = "subscriber";
		conf.set("cassandra.input.thrift.port", "9160");
		conf.set("cassandra.input.thrift.address", "127.0.0.1");
		conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
		conf.set("cassandra.input.keyspace", KEYSPACE);
		conf.set("cassandra.input.columnfamily", COLUMN_FAMILY);
		Class inputFormatClassName = org.apache.cassandra.hadoop.cql3.CqlInputFormat.class;
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
		Class keyClass = java.lang.Long.class;
		conf.setClass("key.class", keyClass, Object.class);
		Class valueClass = com.datastax.driver.core.Row.class;
		conf.setClass("value.class", valueClass, Object.class);

		SimpleFunction<Row, String> myValueTranslateFunc = new SimpleFunction<Row, String>() {
			@Override
			public String apply(Row input) {
				return input.getString("subscriber_email");
			}
		};

		SimpleFunction<Long, String> myKeyTranslateFunc = new SimpleFunction<Long, String>() {
			@Override
			public String apply(Long input) {
				return input.toString();
			}
		};

		HadoopInputFormatIO.Read read = HadoopInputFormatIO.read().withConfiguration(conf)
				.withKeyTranslation(myKeyTranslateFunc).withValueTranslation(myValueTranslateFunc);

		assertEquals("9160", read.getConfiguration().getConfiguration().get("cassandra.input.thrift.port"));
		assertEquals("Murmur3Partitioner",
				read.getConfiguration().getConfiguration().get("cassandra.input.partitioner.class"));
		assertEquals(myKeyTranslateFunc, read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslateFunc, read.getSimpleFuncForValueTranslation());
	}

	@Test
	public void testReadBuildsCorrectlyInDifferentOrder() {

		Configuration conf = new Configuration();
		String KEYSPACE = "mobile_data_usage";
		String COLUMN_FAMILY = "subscriber";
		conf.set("cassandra.input.thrift.port", "9160");
		conf.set("cassandra.input.thrift.address", "127.0.0.1");
		conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
		conf.set("cassandra.input.keyspace", KEYSPACE);
		conf.set("cassandra.input.columnfamily", COLUMN_FAMILY);
		Class<CqlInputFormat> inputFormatClassName = org.apache.cassandra.hadoop.cql3.CqlInputFormat.class;
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
		Class<Long> keyClass = java.lang.Long.class;
		conf.setClass("key.class", keyClass, Object.class);
		Class<Row> valueClass = com.datastax.driver.core.Row.class;
		conf.setClass("value.class", valueClass, Object.class);

		SimpleFunction<Row, String> myValueTranslateFunc = new SimpleFunction<Row, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String apply(Row input) {
				return input.getString("subscriber_email");
			}
		};

		SimpleFunction<Long, String> myKeyTranslateFunc = new SimpleFunction<Long, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String apply(Long input) {
				return input.toString();
			}
		};

		HadoopInputFormatIO.Read read = HadoopInputFormatIO.read().withValueTranslation(myValueTranslateFunc)
				.withConfiguration(conf).withKeyTranslation(myKeyTranslateFunc);

		assertEquals("9160", read.getConfiguration().getConfiguration().get("cassandra.input.thrift.port"));
		assertEquals("Murmur3Partitioner",
				read.getConfiguration().getConfiguration().get("cassandra.input.partitioner.class"));
		assertEquals(myKeyTranslateFunc, read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslateFunc, read.getSimpleFuncForValueTranslation());
	}

	@Test
	public void testReadDisplayData() {

		Configuration conf = new Configuration();
		String KEYSPACE = "mobile_data_usage";
		String COLUMN_FAMILY = "subscriber";
		conf.set("cassandra.input.thrift.port", "9160");
		conf.set("cassandra.input.thrift.address", "127.0.0.1");
		conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
		conf.set("cassandra.input.keyspace", KEYSPACE);
		conf.set("cassandra.input.columnfamily", COLUMN_FAMILY);
		Class<CqlInputFormat> inputFormatClassName = org.apache.cassandra.hadoop.cql3.CqlInputFormat.class;
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
		Class<Long> keyClass = java.lang.Long.class;
		conf.setClass("key.class", keyClass, Object.class);
		Class<Row> valueClass = com.datastax.driver.core.Row.class;
		conf.setClass("value.class", valueClass, Object.class);
		SimpleFunction<Row, String> myValueTranslateFunc = new SimpleFunction<Row, String>() {
			@Override
			public String apply(Row input) {
				return input.getString("subscriber_email");
			}
		};
		SimpleFunction<Long, String> myKeyTranslateFunc = new SimpleFunction<Long, String>() {
			@Override
			public String apply(Long input) {
				return input.toString();
			}
		};
		HadoopInputFormatIO.Read read = HadoopInputFormatIO.read().withValueTranslation(myValueTranslateFunc)
				.withConfiguration(conf).withKeyTranslation(myKeyTranslateFunc);
		DisplayData displayData = DisplayData.from(read);
		if (conf != null) {
			Iterator<Entry<String, String>> propertyElement = conf.iterator();
			while (propertyElement.hasNext()) {
				Entry<String, String> element = propertyElement.next();
				// assertThat(displayData, hasDisplayItem(element.getKey(),
				// element.getValue()));
			}
		}

	}

	@Test
	public void TestReadingData() throws Exception {

		PipelineOptions options = PipelineOptionsFactory.create();
		Configuration conf = new Configuration();
		Class<DummyInputFormat> inputFormatClassName = DummyInputFormat.class;
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
		Class<String> keyClass = java.lang.String.class;
		conf.setClass("key.class", keyClass, Object.class);
		Class<String> valueClass = java.lang.String.class;
		conf.setClass("value.class", valueClass, Object.class);
		final SerializableConfiguration serConf = new SerializableConfiguration(conf);
		Coder<?> keyCoder = StringUtf8Coder.of();
		Coder<?> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(
				serConf, keyCoder, valueCoder);
		long estimatedSize = parentHIFSource.getEstimatedSizeBytes(options);
		List<BoundedSource<KV<String, String>>> boundedSourceList = (List<BoundedSource<KV<String, String>>>) parentHIFSource
				.splitIntoBundles(0, options);
		List<KV<String, String>> referenceRecords = getDummyDataOfRecordReader();
		List<KV<String, String>> bundleRecords = new ArrayList<>();
		for (BoundedSource source : boundedSourceList) {
			List<KV<String, String>> elems = SourceTestUtils.readFromSource(source, options);
			bundleRecords.addAll(elems);
		}
		assertThat(bundleRecords, containsInAnyOrder(referenceRecords.toArray()));
	}

	/*
	 * @Test public void TestReadingDataFromFile() throws Exception {
	 * 
	 * PipelineOptions options = PipelineOptionsFactory.create(); Configuration
	 * conf = new Configuration(); Configuration conf1 = new Configuration();
	 * conf1.set("mapred.input.dir", StringUtils.escapeString("D:\\2.txt"));
	 * conf1.set("mapred.max.split.size", "500"); Class<Text> keyClass =
	 * Text.class; conf1.setClass("key.class", keyClass, Object.class);
	 * Class<Text> valueClass = Text.class; conf1.setClass("value.class",
	 * valueClass, Object.class);
	 * 
	 * final SerializableConfiguration serConf = new
	 * SerializableConfiguration(conf1); Coder<?> keyCoder =
	 * StringUtf8Coder.of(); Coder<?> valueCoder = StringUtf8Coder.of();
	 * SimpleFunction<Text, String> myValueTranslate = new SimpleFunction<Text,
	 * String>() {
	 * 
	 * @Override public String apply(Text input) { return input.toString(); } };
	 * SimpleFunction<LongWritable, String> myKeyTranslate = new
	 * SimpleFunction<LongWritable, String>() {
	 * 
	 * @Override public String apply(LongWritable input) { return
	 * input.toString(); } }; HadoopInputFormatBoundedSource<String, String>
	 * parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(
	 * serConf, keyCoder, valueCoder, myKeyTranslate, myValueTranslate, null);
	 * long estimatedSize = parentHIFSource.getEstimatedSizeBytes(options);
	 * List<BoundedSource<KV<String, String>>> boundedSourceList =
	 * (List<BoundedSource<KV<String, String>>>) parentHIFSource
	 * .splitIntoBundles(0, options); List<KV<String, String>> referenceRecords
	 * = getDummyDataOfRecordReader(); List<KV<String, String>> bundleRecords =
	 * new ArrayList<>(); for (BoundedSource source : boundedSourceList) {
	 * List<KV<String, String>> elems = SourceTestUtils.readFromSource(source,
	 * options); bundleRecords.addAll(elems); } assertThat(bundleRecords,
	 * containsInAnyOrder(referenceRecords.toArray())); }
	 */

	private List<KV<String, String>> getDummyDataOfRecordReader() {
		List<KV<String, String>> data = new ArrayList();
		data.add(KV.of("0", "Chaitanya"));
		data.add(KV.of("1", "Rahul"));
		data.add(KV.of("2", "Singh"));
		data.add(KV.of("3", "Ajeet"));
		data.add(KV.of("4", "Anuj"));
		data.add(KV.of("5", "xyz"));
		data.add(KV.of("6", "persistent"));
		data.add(KV.of("7", "apache"));
		data.add(KV.of("8", "beam"));

		return data;
	}

	@Test
	public void testReaderGetFractionConsumed() {

	}

}
