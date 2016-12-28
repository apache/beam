package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasKey;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasLabel;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasValue;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;







import java.io.IOException;
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
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.DummyBadInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.DummyBadInputFormat2;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.DummyInputFormat;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Row;

public class HadoopInputFormatIOTest {
	SerializableConfiguration serConf;
	private DirectOptions directRunnerOptions;
	private Pipeline pipeline ;
	Configuration conf ;
	SimpleFunction<Text, String> myKeyTranslate;
	SimpleFunction<Text, String> myValueTranslate;
	PBegin input;

	@Before
	public void setUp() {
		if(directRunnerOptions == null)
			directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
		if(pipeline == null){
			pipeline = Pipeline.create(directRunnerOptions);
			input = PBegin.in(pipeline);
		}
		if(conf == null)
			conf  = getConfiguration(DummyInputFormat.class,Text.class,Text.class).getConfiguration();
		if(myKeyTranslate == null)
			myKeyTranslate = new SimpleFunction<Text, String>() {
			private static final long serialVersionUID = 1L;
			@Override 
			public String apply(Text input) { 
				return input.toString(); 
			} 
		};

		if(myValueTranslate == null)
			myValueTranslate = new SimpleFunction<Text, String>(){
			private static final long serialVersionUID = 1L;
			@Override 
			public String apply(Text input){
				return input.toString();
			}
		};
	}


	@Test
	public void testReadBuildsCorrectly() {
		Configuration conf = getConfiguration(DummyInputFormat.class,java.lang.Long.class,Text.class).getConfiguration();
		SimpleFunction<Long, String> myKeyTranslateFunc = new SimpleFunction<Long, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Long input) {
				return input.toString();
			}
		};
		SimpleFunction<Text, String> myValueTranslateFunc = new SimpleFunction<Text, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Text input) {
				return input.toString();
			}
		};
		HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withConfiguration(conf)
				.withKeyTranslation(myKeyTranslateFunc)
				.withValueTranslation(myValueTranslateFunc);
		assertEquals(DummyInputFormat.class, read.getConfiguration().getConfiguration().getClass("mapreduce.job.inputformat.class",Object.class));
		assertEquals(java.lang.Long.class,read.getConfiguration().getConfiguration().getClass("key.class",Object.class));
		assertEquals(Text.class,read.getConfiguration().getConfiguration().getClass("value.class",Object.class));
		assertEquals(myKeyTranslateFunc, read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslateFunc, read.getSimpleFuncForValueTranslation());
	}

	@Test
	public void testReadBuildsCorrectlyInDifferentOrder() {
		Configuration conf = getConfiguration(DummyInputFormat.class,java.lang.Long.class,Text.class).getConfiguration();
		SimpleFunction<Long, String> myKeyTranslateFunc = new SimpleFunction<Long, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Long input) {
				return input.toString();
			}
		};
		SimpleFunction<Text, String> myValueTranslateFunc = new SimpleFunction<Text, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Text input) {
				return input.toString();
			}
		};
		HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withValueTranslation(myValueTranslateFunc)
				.withConfiguration(conf)
				.withKeyTranslation(myKeyTranslateFunc);
		assertEquals(DummyInputFormat.class, read.getConfiguration().getConfiguration().getClass("mapreduce.job.inputformat.class",Object.class));
		assertEquals(java.lang.Long.class,read.getConfiguration().getConfiguration().getClass("key.class",Object.class));
		assertEquals(Text.class,read.getConfiguration().getConfiguration().getClass("value.class",Object.class));
		assertEquals(myKeyTranslateFunc, read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslateFunc, read.getSimpleFuncForValueTranslation());
	}



	// This test validates Read transform object creation if only withConfiguration() is called with null value.
	// withConfiguration() checks configuration is null or not and throws exception if null value is send to withConfiguration().
	@Test
	public void testReadObjectCreationWithOnlyConfigurationAndNullIsPassed() {
		try{
			HadoopInputFormatIO.<String, String>read()
			.withConfiguration(null);
		}
		catch(NullPointerException ex)
		{
			assertEquals("Configuration cannot be null.",ex.getMessage());
		}
	}


	// This test validates Read transform object creation if only withConfiguration() is called.
	@Test
	public void testReadObjectCreationWithOnlyConfiguration() {
		Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withConfiguration(conf);
		read.validate(input);
		assertEquals(conf,read.getConfiguration().getConfiguration());
		assertEquals(null,read.getSimpleFuncForKeyTranslation());
		assertEquals(null,read.getSimpleFuncForValueTranslation());
		assertEquals(conf.getClass("key.class", Object.class),read.getKeyClass());
		assertEquals(conf.getClass("value.class", Object.class),read.getValueClass());

	}

	// This test validates behaviour Read transform object creation if withConfiguration() and withKeyTranslation() are called and null value is passed to kayTranslation.
	// withKeyTranslation() checks keyTranslation is null or not and throws exception if null value is send to withKeyTranslation().
	@Test
	public void testReadObjectCreationWithConfigurationKeyTranslationIfKeyTranslationIsNull() {
		try{
			HadoopInputFormatIO.<String, String>read()
			.withConfiguration(conf)
			.withKeyTranslation(null);
		}
		catch(NullPointerException ex)
		{
			assertEquals("Simple function for key translation cannot be null.",ex.getMessage());
		}
		
	}


	// This test validates Read transform object creation if withConfiguration() and withKeyTranslation() are called.
	@Test
	public void testReadObjectCreationWithConfigurationKeyTranslation() {
		Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withConfiguration(conf)
				.withKeyTranslation(myKeyTranslate);
		read.validate(input);
		assertEquals(conf,read.getConfiguration().getConfiguration());
		assertEquals(myKeyTranslate,read.getSimpleFuncForKeyTranslation());
		assertEquals(null,read.getSimpleFuncForValueTranslation());
		assertEquals(myKeyTranslate.getOutputTypeDescriptor().getRawType(),read.getKeyClass());
		assertEquals(conf.getClass("value.class", Object.class),read.getValueClass());
	}

	// This test validates behaviour Read transform object creation if withConfiguration() and withValueTranslation() are called and null value is passed to valueTranslation.
	// withValueTranslation() checks valueTranslation is null or not and throws exception if null value is send to withValueTranslation().
	@Test
	public void testReadObjectCreationWithConfigurationValueTranslationIfValueTranslationIsNull() {
		try{
			HadoopInputFormatIO.<String, String>read()
			.withConfiguration(conf)
			.withValueTranslation(null);
		}
		catch(NullPointerException ex)
		{
			assertEquals("Simple function for value translation cannot be null.",ex.getMessage());
		}
		
	}

	// This test validates Read transform object creation if withConfiguration() and withValueTranslation() are called.
	@Test
	public void testReadObjectCreationWithConfigurationValueTranslation() {
		Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withConfiguration(conf)
				.withValueTranslation(myValueTranslate);
		read.validate(input);
		assertEquals(conf,read.getConfiguration().getConfiguration());
		assertEquals(null,read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslate,read.getSimpleFuncForValueTranslation());
		assertEquals(conf.getClass("key.class", Object.class),read.getKeyClass());
		assertEquals(myValueTranslate.getOutputTypeDescriptor().getRawType(),read.getValueClass());
	}


	// This test validates Read transform object creation if withConfiguration() , withKeyTranslation() and withValueTranslation() are called.
	@Test
	public void testReadObjectCreationWithConfigurationKeyTranslationValueTranslation() {

		Read<String, String> read = HadoopInputFormatIO.<String, String>read()
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
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Row input) {
				return input.getString("subscriber_email");
			}
		};
		SimpleFunction<Long, String> myKeyTranslateFunc = new SimpleFunction<Long, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Long input) {
				return input.toString();
			}
		};
		HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withValueTranslation(myValueTranslateFunc)
				.withConfiguration(conf)
				.withKeyTranslation(myKeyTranslateFunc);
		DisplayData displayData = DisplayData.from(read);
		if (conf != null) {
			Iterator<Entry<String, String>> propertyElement = conf.iterator();
			while (propertyElement.hasNext()) {
				Entry<String, String> element = propertyElement.next();
				assertThat(displayData, hasDisplayItem(element.getKey(),element.getValue()));
			}
		}

	}

	///This test validates functionality of Read.validate() function when Read transform is created without calling withConfiguration().
	@Test
	public void testReadObjectCreationIfWithConfigurationIsNotCalled() {
		DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
		Pipeline pipeline = Pipeline.create(directRunnerOptions);
		Read<String, String> read = HadoopInputFormatIO.<String, String>read();
		PBegin input = PBegin.in(pipeline);
		try{
			read.validate(input);
		}
		catch(NullPointerException ex)
		{
			assertEquals( "Need to set the configuration of a HadoopInputFormatIO Read using method Read.withConfiguration().",ex.getMessage());
		}
		
	}


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
		Read<String, String> read = HadoopInputFormatIO.<String, String>read()
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
		Read<String, String> read = HadoopInputFormatIO.<String, String>read()
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
		Read<String, String> read = HadoopInputFormatIO.<String, String>read()
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
		Configuration conf =getConfiguration(DummyInputFormat.class,Text.class,java.lang.String.class).getConfiguration(); 
		SimpleFunction<LongWritable, String> myKeyTranslate = new SimpleFunction<LongWritable, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(LongWritable input) { 
				return input.toString(); 
			} 
		};
		Read<String, String> read = HadoopInputFormatIO.<String, String>read()
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
		Configuration conf = getConfiguration(DummyInputFormat.class,java.lang.String.class,Text.class).getConfiguration();
		SimpleFunction<LongWritable, String> myValueTranslate = new SimpleFunction<LongWritable, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(LongWritable input) { 
				return input.toString(); 
			} 
		};
		Read<String, String> read = HadoopInputFormatIO.<String, String>read()
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
		serConf = getConfiguration(DummyInputFormat.class,java.lang.String.class,java.lang.String.class); 
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(
				serConf, keyCoder, valueCoder);
		List<BoundedSource<KV<String, String>>> boundedSourceList = parentHIFSource.splitIntoBundles(0, options);
		List<KV<String, String>> referenceRecords = getDummyDataOfRecordReader();
		List<KV<String, String>> bundleRecords = new ArrayList<>();
		for (BoundedSource<KV<String, String>> source : boundedSourceList) {
			List<KV<String, String>> elems = SourceTestUtils.readFromSource(source, options);
			bundleRecords.addAll(elems);
		}
		assertThat(bundleRecords, containsInAnyOrder(referenceRecords.toArray()));
	}

	@Test
	public void testReadersGetFractionConsumed() throws Exception {
		long inputDataSize=9,numberOfSplitsOfDummyInputFormat=3;
		PipelineOptions options = PipelineOptionsFactory.create();
		serConf = getConfiguration(DummyInputFormat.class,java.lang.String.class,java.lang.String.class); 
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		long estimatedSize = parentHIFSource.getEstimatedSizeBytes(options);
		assertEquals(inputDataSize,estimatedSize);//
		List<BoundedSource<KV<String, String>>> boundedSourceList = (List<BoundedSource<KV<String, String>>>) parentHIFSource.splitIntoBundles(0, options);
		assertEquals(numberOfSplitsOfDummyInputFormat,boundedSourceList.size());
		List<KV<String, String>> referenceRecords = getDummyDataOfRecordReader();
		List<KV<String, String>> bundleRecords = new ArrayList<>();
		for (BoundedSource<KV<String, String>> source : boundedSourceList) {
			List<KV<String, String>> elements = new ArrayList<KV<String, String>>();
			BoundedReader<KV<String, String>> reader=source.createReader(options);
			assertEquals(new Double((float)0),reader.getFractionConsumed());

			reader.start();
			elements.add(reader.getCurrent());
			assertEquals(new Double((float)1/3),reader.getFractionConsumed());

			reader.advance();
			elements.add(reader.getCurrent());
			assertEquals(new Double((float)2/3),reader.getFractionConsumed());

			reader.advance();
			elements.add(reader.getCurrent());
			assertEquals(new Double((float)1),reader.getFractionConsumed());

			bundleRecords.addAll(elements);
		}
		assertThat(bundleRecords, containsInAnyOrder(referenceRecords.toArray()));
	}

	//Validate that the Reader and its parent source reads the same records.
	@Test
	public void testReaderAndParentSourceReadsSameData() throws Exception 
	{
		PipelineOptions options = PipelineOptionsFactory.create();
		serConf = getConfiguration(DummyInputFormat.class,java.lang.String.class,java.lang.String.class); 
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		List<BoundedSource<KV<String, String>>> boundedSourceList = (List<BoundedSource<KV<String, String>>>) parentHIFSource.splitIntoBundles(0, options);
		for (BoundedSource<KV<String, String>> source : boundedSourceList) {
			BoundedReader<KV<String, String>> reader=source.createReader(options);
			SourceTestUtils.assertUnstartedReaderReadsSameAsItsSource(reader, options);
		}

	}


	//This test verifies that the method HadoopInputFormatReader.getCurrentSource() returns correct source object.
	@Test
	public void testGetCurrentSourceFunc() throws Exception 
	{
		PipelineOptions options = PipelineOptionsFactory.create();
		serConf = getConfiguration(DummyInputFormat.class,java.lang.String.class,java.lang.String.class); 
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		List<BoundedSource<KV<String, String>>> boundedSourceList = (List<BoundedSource<KV<String, String>>>) parentHIFSource
				.splitIntoBundles(0, options);
		for (BoundedSource<KV<String, String>> source : boundedSourceList) {
			BoundedReader<KV<String, String>> HIFReader=source.createReader(options);
			BoundedSource<KV<String, String>> HIFSource = HIFReader.getCurrentSource();
			assertEquals(HIFSource,source);
		}

	}


	//This test validates behaviour of HadoopInputFormatSource.createReader() method when HadoopInputFormatSource.splitIntoBundles() is not called.
	@Test
	public void testCreateReaderIfSplitIntoBundlesNotCalled() throws Exception 
	{
		PipelineOptions options = PipelineOptionsFactory.create();
		serConf = getConfiguration(DummyInputFormat.class,java.lang.String.class,java.lang.String.class); 
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		try{
			parentHIFSource.createReader(options);
		}
		catch(IOException ex){
			assertEquals("Cannot create reader as source is not split yet.",ex.getMessage());
		}

	}


	//This test validates behavior of getEstimatedSizeBytes() and splitIntoBundles() when Hadoop InputFormat's getSplits() returns empty list.
	@Test
	public void testSplitIntoBundlesIfGetSplitsReturnsEmptyList() throws Exception 
	{
		PipelineOptions options = PipelineOptionsFactory.create();
		serConf = getConfiguration(DummyBadInputFormat.class,java.lang.String.class,java.lang.String.class); 
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		try{
			parentHIFSource.getEstimatedSizeBytes(options);
		}
		catch(IOException ex)
		{
			assertEquals("Cannot split the source as getSplits() is returning empty list.",ex.getMessage());
		}
		try{
			parentHIFSource.splitIntoBundles(0, options);
		}
		catch(IOException ex)
		{
			assertEquals("Cannot split the source as getSplits() is returning empty list.",ex.getMessage());
		}

	}


	//This test validates behavior of getEstimatedSizeBytes() and splitIntoBundles() when Hadoop InputFormat's getSplits() returns NULL value.
	@Test
	public void testSplitIntoBundlesIfGetSplitsReturnsNullValue() throws Exception 
	{
		PipelineOptions options = PipelineOptionsFactory.create();
		serConf = getConfiguration(DummyBadInputFormat2.class,java.lang.String.class,java.lang.String.class); 
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		try{
			parentHIFSource.getEstimatedSizeBytes(options);
		}
		catch(IOException ex)
		{
			assertEquals("Cannot split the source as getSplits() is returning null value.",ex.getMessage());
		}
		try{
			parentHIFSource.splitIntoBundles(0, options);
		}
		catch(IOException ex)
		{
			assertEquals("Cannot split the source as getSplits() is returning null value.",ex.getMessage());
		}

	}

	//Verify a scenario where the computeSplits () method returns empty List<InputSplit>
	@Test
	public void test() throws Exception 
	{
		PipelineOptions options = PipelineOptionsFactory.create();
		Configuration conf = new Configuration();
		Class<DummyInputFormat> inputFormatClassName = DummyInputFormat.class;
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
		Class<String> keyClass = java.lang.String.class;
		conf.setClass("key.class", keyClass, Object.class);
		Class<String> valueClass = java.lang.String.class;
		conf.setClass("value.class", valueClass, Object.class);
		final SerializableConfiguration serConf = new SerializableConfiguration(conf);
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		try{
			parentHIFSource.createReader(options);
		}
		catch(IOException ex){
			assertEquals("Cannot create reader as source is not split yet.",ex.getMessage());
		}

	}




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


	private SerializableConfiguration getConfiguration(Class<?> inputFormatClassName,Class<?> keyClass,Class<?> valueClass){
		Configuration conf = new Configuration();
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
		conf.setClass("key.class", keyClass, Object.class);
		conf.setClass("value.class", valueClass, Object.class);
		return new SerializableConfiguration(conf);
	}
}
