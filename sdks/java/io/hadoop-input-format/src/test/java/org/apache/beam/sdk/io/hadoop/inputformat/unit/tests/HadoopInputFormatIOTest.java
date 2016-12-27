package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
//import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.HadoopInputFormatBoundedSource;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.Read;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.SerializableConfiguration;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
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

		HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.read()
				.withConfiguration(conf)
				.withKeyTranslation(myKeyTranslateFunc)
				.withValueTranslation(myValueTranslateFunc);

		assertEquals("9160", read.getConfiguration().getConfiguration().get("cassandra.input.thrift.port"));
		assertEquals("Murmur3Partitioner",read.getConfiguration().getConfiguration().get("cassandra.input.partitioner.class"));
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

		HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.read().withValueTranslation(myValueTranslateFunc)
				.withConfiguration(conf).withKeyTranslation(myKeyTranslateFunc);

		assertEquals("9160", read.getConfiguration().getConfiguration().get("cassandra.input.thrift.port"));
		assertEquals("Murmur3Partitioner",
				read.getConfiguration().getConfiguration().get("cassandra.input.partitioner.class"));
		assertEquals(myKeyTranslateFunc, read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslateFunc, read.getSimpleFuncForValueTranslation());
	}

	//This test validates creation of Read transform object.
	@Test 
	public void testReadObjectCreation(){
		DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
		Pipeline pipeline = Pipeline.create(directRunnerOptions);
		Configuration conf = new Configuration();
		Class<DummyInputFormat> inputFormatClassName = DummyInputFormat.class;
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
		Class<Text> keyClass = Text.class;
		conf.setClass("key.class", keyClass, Object.class);
		Class<Text> valueClass = Text.class;
		conf.setClass("value.class", valueClass, Object.class);
		SimpleFunction<Text, String> myValueTranslate = new SimpleFunction<Text, String>(){
			@Override 
			public String apply(Text input){
				return input.toString();
			}
		};
		SimpleFunction<Text, String> myKeyTranslate = new SimpleFunction<Text, String>() {
			@Override 
			public String apply(Text input) { 
				return input.toString(); 
			} 
		};

		//Read object is created without calling withConfiguration(). Exception is thrown in validate() method as configuration must be set by user..
		Read<String, String> read = HadoopInputFormatIO.<KV<String, String>> read();
		PBegin input = PBegin.in(pipeline);
		try{
			read.validate(input);
		}
		catch(NullPointerException ex)
		{
			assertEquals("Need to set the configuration of a HadoopInputFormatIO Read using method Read.withConfiguration().",ex.getMessage());
		}

		//Only withConfiguration() called with null value passed. 
		//withConfiguration() checks configuration is null or not and throws exception if null value is send to withConfiguration().
		try{
			read = HadoopInputFormatIO.<KV<String, String>> read()
					.withConfiguration(null);
		}
		catch(NullPointerException ex)
		{
			assertEquals("Configuration cannot be null.",ex.getMessage());
		}

		
		//Only withConfiguration() called.
		read = HadoopInputFormatIO.<KV<String, String>> read()
				.withConfiguration(conf);
		read.validate(input);
		assertEquals(conf,read.getConfiguration().getConfiguration());
		assertEquals(null,read.getSimpleFuncForKeyTranslation());
		assertEquals(null,read.getSimpleFuncForValueTranslation());
		assertEquals(conf.getClass("key.class", Object.class),read.getKeyClass());
		assertEquals(conf.getClass("value.class", Object.class),read.getValueClass());
		
		
		// withConfiguration() and withKeyTranslation() are called. withKeyTranslation() null value is passed as parameter.
		try{read = HadoopInputFormatIO.<KV<String, String>> read()
				.withConfiguration(conf)
				.withKeyTranslation(null);
		}
		catch(NullPointerException ex)
		{
			assertEquals("Simple function for key translation cannot be null.",ex.getMessage());
		}
		

		// withConfiguration() and withKeyTranslation() are called.
		read = HadoopInputFormatIO.<KV<String, String>> read()
				.withConfiguration(conf)
				.withKeyTranslation(myKeyTranslate);
		read.validate(input);
		assertEquals(conf,read.getConfiguration().getConfiguration());
		assertEquals(myKeyTranslate,read.getSimpleFuncForKeyTranslation());
		assertEquals(null,read.getSimpleFuncForValueTranslation());
		assertEquals(myKeyTranslate.getOutputTypeDescriptor().getRawType(),read.getKeyClass());
		assertEquals(conf.getClass("value.class", Object.class),read.getValueClass());

		

		// withConfiguration() and withValueTranslation() are called. withValueTranslation() null value is passed as parameter.
		try{
			read = HadoopInputFormatIO.<KV<String, String>> read()
					.withConfiguration(conf)
					.withValueTranslation(null);
		}
		catch(NullPointerException ex)
		{
			assertEquals("Simple function for value translation cannot be null.",ex.getMessage());
		}
		
		
		// withConfiguration() and withValueTranslation() are called.
		read = HadoopInputFormatIO.<KV<String, String>> read()
				.withConfiguration(conf)
				.withValueTranslation(myValueTranslate);
		read.validate(input);
		assertEquals(conf,read.getConfiguration().getConfiguration());
		assertEquals(null,read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslate,read.getSimpleFuncForValueTranslation());
		assertEquals(conf.getClass("key.class", Object.class),read.getKeyClass());
		assertEquals(myValueTranslate.getOutputTypeDescriptor().getRawType(),read.getValueClass());
		
		// withConfiguration() , withKeyTranslation() and withValueTranslation() are called.
		read = HadoopInputFormatIO.<KV<String, String>> read()
				.withConfiguration(conf)
				.withKeyTranslation(myKeyTranslate)
				.withValueTranslation(myValueTranslate);
		read.validate(input);
		assertEquals(conf,read.getConfiguration().getConfiguration());
		assertEquals(myKeyTranslate,read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslate,read.getSimpleFuncForValueTranslation());
		assertEquals(myKeyTranslate.getOutputTypeDescriptor().getRawType(),read.getKeyClass());
		assertEquals(myValueTranslate.getOutputTypeDescriptor().getRawType(),read.getValueClass());
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
		HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.read().withValueTranslation(myValueTranslateFunc)
				.withConfiguration(conf).withKeyTranslation(myKeyTranslateFunc);
		DisplayData displayData = DisplayData.from(read);
		if (conf != null) {
			Iterator<Entry<String, String>> propertyElement = conf.iterator();
			while (propertyElement.hasNext()) {
				Entry<String, String> element = propertyElement.next();
				// assertThat(displayData, DisplayDataMatchers.hasDisplayItem(element.getKey(),element.getValue()));
			}
		}

	}

	/*///This test validates functionality of Read.validate() function when Read transform is created without calling withConfiguration(conf).
	@Test
	public void testIfWithConfigurationIsNotCalled() {
		DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
		Pipeline pipeline = Pipeline.create(directRunnerOptions);
		Configuration conf =null;
		Read<String, String> read = HadoopInputFormatIO.<KV<String, String>> read();
		PBegin input = PBegin.in(pipeline);
		try{
			read.validate(input);
		}
		catch(NullPointerException ex)
		{
			assertEquals( "Need to set the configuration of a HadoopInputFormatIO Read using method Red.withConfiguration().",ex.getMessage());
		}
	}*/


	//This test validates functionality of Read.validate() function when Hadoop InputFormat class is not provided by user in configuration.
	@Test
	public void testIfInputFormatIsNotProvided() {

		DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
		Pipeline pipeline = Pipeline.create(directRunnerOptions);
		Configuration conf = new Configuration();
		Class<String> keyClass = java.lang.String.class;
		conf.setClass("key.class", keyClass, Object.class);
		Class<String> valueClass = java.lang.String.class;
		conf.setClass("value.class", valueClass, Object.class);
		final SerializableConfiguration serConf = new SerializableConfiguration(conf);
		Read<String, String> read = HadoopInputFormatIO.<KV<String, String>> read()
				.withConfiguration(conf);
		PBegin input = PBegin.in(pipeline);
		try{
			read.validate(input);
		}
		catch(IllegalArgumentException ex)
		{
			assertEquals("Hadoop InputFormat class property \"mapreduce.job.inputformat.class\" is not set in configuration.",ex.getMessage());
		}

	}

	//This test validates functionality of Read.validate() function when key class is not provided by user in configuration.
	@Test
	public void testIfKeyClassIsNotProvided() {

		DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
		Pipeline pipeline = Pipeline.create(directRunnerOptions);
		Configuration conf = new Configuration();
		Class<DummyInputFormat> inputFormatClassName = DummyInputFormat.class;
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
		Class<String> valueClass = java.lang.String.class;
		conf.setClass("value.class", valueClass, Object.class);
		final SerializableConfiguration serConf = new SerializableConfiguration(conf);
		Read<String, String> read = HadoopInputFormatIO.<KV<String, String>> read()
				.withConfiguration(conf);
		PBegin input = PBegin.in(pipeline);
		try{
			read.validate(input);
		}
		catch(IllegalArgumentException ex)
		{
			assertEquals("Configuration property \"key.class\" is not set.",ex.getMessage());
		}

	}

	//This test validates functionality of Read.validate() function when value class is not provided by user in configuration.
	@Test
	public void testIfValueClassIsNotProvided() {

		DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
		Pipeline pipeline = Pipeline.create(directRunnerOptions);
		Configuration conf = new Configuration();
		Class<DummyInputFormat> inputFormatClassName = DummyInputFormat.class;
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
		Class<String> keyClass = java.lang.String.class;
		conf.setClass("key.class", keyClass, Object.class);
		final SerializableConfiguration serConf = new SerializableConfiguration(conf);
		Read<String, String> read = HadoopInputFormatIO.<KV<String, String>> read()
				.withConfiguration(conf);
		PBegin input = PBegin.in(pipeline);
		try{
			read.validate(input);
		}
		catch(IllegalArgumentException ex)
		{
			assertEquals("Configuration property \"value.class\" is not set.",ex.getMessage());
		}

	}



	//This test validates functionality of Read.validate() function when myKeyTranslate's (simple function provided by user for key translation) 
	//input type is not same as hadoop input format's keyClass(Which is property set in configuration as "key.class").
	@Test
	public void testKeyTranslationFunctionIfInputTypeIsWrong() {
		DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
		Pipeline pipeline = Pipeline.create(directRunnerOptions);
		Configuration conf = new Configuration();
		Class<DummyInputFormat> inputFormatClassName = DummyInputFormat.class;
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
		Class<String> keyClass = java.lang.String.class;
		conf.setClass("key.class", keyClass, Object.class);
		Class<String> valueClass = java.lang.String.class;
		conf.setClass("value.class", valueClass, Object.class);
		SimpleFunction<LongWritable, String> myKeyTranslate = new SimpleFunction<LongWritable, String>() {
			@Override
			public String apply(LongWritable input) { 
				return input.toString(); 
			} 
		};
		Read<String, String> read = HadoopInputFormatIO.<KV<String, String>> read()
				.withConfiguration(conf)
				.withKeyTranslation(myKeyTranslate);
		PBegin input = PBegin.in(pipeline);
		try{
			read.validate(input);
		}
		catch(IllegalArgumentException ex)
		{
			String inputFormatClassProperty= conf.get("mapreduce.job.inputformat.class") ;
			String keyClassProperty= conf.get("key.class");
			String expectedMessage="Key translation's input type is not same as hadoop input format : "+inputFormatClassProperty+" key class : "+ keyClassProperty;
			assertEquals(expectedMessage,ex.getMessage());
		}
	}

	//This test validates functionality of Read.validate() function when myValueTranslate's (simple function provided by user for value translation) 
	//input type is not same as hadoop input format's valueClass(Which is property set in configuration as "value.class").
	@Test
	public void testValueTranslationFunctionIfInputTypeIsWrong() {
		DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
		Pipeline pipeline = Pipeline.create(directRunnerOptions);
		Configuration conf = new Configuration();
		Class<DummyInputFormat> inputFormatClassName = DummyInputFormat.class;
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
		Class<String> keyClass = java.lang.String.class;
		conf.setClass("key.class", keyClass, Object.class);
		Class<String> valueClass = java.lang.String.class;
		conf.setClass("value.class", valueClass, Object.class);
		SimpleFunction<LongWritable, String> myValueTranslate = new SimpleFunction<LongWritable, String>() {
			@Override
			public String apply(LongWritable input) { 
				return input.toString(); 
			} 
		};
		final SerializableConfiguration serConf = new SerializableConfiguration(conf);
		Read<String, String> read = HadoopInputFormatIO.<KV<String, String>> read()
				.withConfiguration(conf)
				.withValueTranslation(myValueTranslate);
		PBegin input = PBegin.in(pipeline);
		try{
			read.validate(input);
		}
		catch(IllegalArgumentException ex)
		{
			String inputFormatClassProperty= conf.get("mapreduce.job.inputformat.class") ;
			String keyClassProperty= conf.get("value.class");
			String expectedMessage="Value translation's input type is not same as hadoop input format : "+inputFormatClassProperty+" value class : "+ keyClassProperty;
			assertEquals(expectedMessage,ex.getMessage());
		}
	}

	@Test
	public void testReadingData()  throws Exception {
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

	@Test
	public void testReadersGetFractionConsumed() throws Exception {

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
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		long estimatedSize = parentHIFSource.getEstimatedSizeBytes(options);
		assertEquals(9,estimatedSize);//
		List<BoundedSource<KV<String, String>>> boundedSourceList = (List<BoundedSource<KV<String, String>>>) parentHIFSource.splitIntoBundles(0, options);
		assertEquals(3,boundedSourceList.size());
		List<KV<String, String>> referenceRecords = getDummyDataOfRecordReader();
		List<KV<String, String>> bundleRecords = new ArrayList<>();
		for (BoundedSource source : boundedSourceList) {
			List<KV<String, String>> elements = new ArrayList<KV<String, String>>();
			BoundedReader reader=source.createReader(options);
			assertEquals(new Double((float)0),reader.getFractionConsumed());

			reader.start();
			elements.add((KV<String, String>) reader.getCurrent());
			assertEquals(new Double((float)1/3),reader.getFractionConsumed());

			reader.advance();
			elements.add((KV<String, String>) reader.getCurrent());
			assertEquals(new Double((float)2/3),reader.getFractionConsumed());

			reader.advance();
			elements.add((KV<String, String>) reader.getCurrent());
			assertEquals(new Double((float)1),reader.getFractionConsumed());

			bundleRecords.addAll(elements);
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
		List<KV<String, String>> data = new ArrayList<KV<String, String>>();
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


}
