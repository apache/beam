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
package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.HadoopInputFormatBoundedSource;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.Read;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.SerializableConfiguration;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.coders.EmployeeCoder;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.BadCreateRecordReaderInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.EmptyInputSplitsInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.NullInputSplitsBadInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.BadRecordReaderNoRecordsInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.DummyInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.ImmutableRecordsInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.MutableRecordsInputFormat;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.collect.Lists;


/**
 * Unit tests for {@link HadoopInputFormatIO}.
 */
@RunWith(JUnit4.class)
public class HadoopInputFormatIOTest {
	SerializableConfiguration serConf;
	private static DirectOptions directRunnerOptions;
	private static Pipeline pipeline ;
	static PipelineOptions options ;
	static Configuration conf ;
	static SimpleFunction<Text, String> myKeyTranslate;
	static SimpleFunction<Text, String> myValueTranslate;
	static PBegin input;

	@Rule public final transient TestPipeline p = TestPipeline.create();
	@Rule public ExpectedException thrown = ExpectedException.none();

	@BeforeClass
	public static void setUp() {
		pipeline = TestPipeline.create();
		input = PBegin.in(pipeline);
		conf  = getConfiguration(TypeDescriptor.of(DummyInputFormat.class),TypeDescriptor.of(Text.class),TypeDescriptor.of(Text.class)).getConfiguration();
		myKeyTranslate = new SimpleFunction<Text, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Text input) {
				return input.toString();
			}
		};

		myValueTranslate = new SimpleFunction<Text, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Text input){
				return input.toString();
			}
		};
	}


	//Read validation
	@Test
	public void testReadBuildsCorrectly() {
		Configuration conf = getConfiguration(TypeDescriptor.of(DummyInputFormat.class),TypeDescriptor.of(java.lang.Long.class),TypeDescriptor.of(Text.class)).getConfiguration();
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
		Configuration conf = getConfiguration(TypeDescriptor.of(DummyInputFormat.class),TypeDescriptor.of(java.lang.Long.class),TypeDescriptor.of(Text.class)).getConfiguration();
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

		thrown.expect(NullPointerException.class);
		thrown.expectMessage("Configuration cannot be null");
		HadoopInputFormatIO.<String, String>read()
		.withConfiguration(null);
	

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
		assertEquals(conf.getClass("key.class", Object.class),read.getKeyClass().getRawType());
		assertEquals(conf.getClass("value.class", Object.class),read.getValueClass().getRawType());

	}

	// This test validates behavior Read transform object creation if withConfiguration() and withKeyTranslation() are called and null value is passed to kayTranslation.
	// withKeyTranslation() checks keyTranslation is null or not and throws exception if null value is send to withKeyTranslation().
	@Test
	public void testReadObjectCreationWithConfigurationKeyTranslationIfKeyTranslationIsNull() {

		thrown.expect(NullPointerException.class);
		thrown.expectMessage("Simple function for key translation cannot be null.");
		HadoopInputFormatIO.<String, String>read()
		.withConfiguration(conf)
		.withKeyTranslation(null);
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
		assertEquals(myKeyTranslate.getOutputTypeDescriptor().getRawType(),read.getKeyClass().getRawType());
		assertEquals(conf.getClass("value.class", Object.class),read.getValueClass().getRawType());
	}

	// This test validates behaviour Read transform object creation if withConfiguration() and withValueTranslation() are called and null value is passed to valueTranslation.
	// withValueTranslation() checks valueTranslation is null or not and throws exception if null value is send to withValueTranslation().
	@Test
	public void testReadObjectCreationWithConfigurationValueTranslationIfValueTranslationIsNull() {

		thrown.expect(NullPointerException.class);
		thrown.expectMessage("Simple function for value translation cannot be null.");
		HadoopInputFormatIO.<String, String>read()
		.withConfiguration(conf)
		.withValueTranslation(null);
	
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
		assertEquals(conf.getClass("key.class", Object.class),read.getKeyClass().getRawType());
		assertEquals(myValueTranslate.getOutputTypeDescriptor().getRawType(),read.getValueClass().getRawType());
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
		assertEquals(myKeyTranslate.getOutputTypeDescriptor().getRawType(),read.getKeyClass().getRawType());
		assertEquals(myValueTranslate.getOutputTypeDescriptor().getRawType(),read.getValueClass().getRawType());
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
		TypeDescriptor<CqlInputFormat> inputFormatClassName = new TypeDescriptor<CqlInputFormat>() {
			private static final long serialVersionUID = 1L;
		};
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName.getRawType(), InputFormat.class);
		TypeDescriptor<Long> keyClass = new TypeDescriptor<Long>() {
			private static final long serialVersionUID = 1L;
		};
		conf.setClass("key.class", keyClass.getRawType(), Object.class);
		TypeDescriptor<com.datastax.driver.core.Row> valueClass = new TypeDescriptor<Row>() {
			private static final long serialVersionUID = 1L;
		};
		conf.setClass("value.class", valueClass.getRawType(), Object.class);
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
		read.validate(input);
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
		Read<String, String> read = HadoopInputFormatIO.<String, String>read();
		PBegin input = PBegin.in(pipeline);
		thrown.expect(NullPointerException.class);
		thrown.expectMessage("Need to set the configuration of a HadoopInputFormatIO Read using method Read.withConfiguration().");
		read.validate(input);
	}


	//This test validates functionality of Read.validate() function when Hadoop InputFormat class is not provided by user in configuration.
	@Test
	public void testIfInputFormatIsNotProvided() {

		Configuration conf = new Configuration();
		TypeDescriptor<String> keyClass = new TypeDescriptor<String>() {
			private static final long serialVersionUID = 1L;
		};
		conf.setClass("key.class", keyClass.getRawType(), Object.class);
		TypeDescriptor<String> valueClass = new TypeDescriptor<String>() {
			private static final long serialVersionUID = 1L;
		};
		conf.setClass("value.class", valueClass.getRawType(), Object.class);
		Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withConfiguration(conf);
		PBegin input = PBegin.in(pipeline);

		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Hadoop InputFormat class property \"mapreduce.job.inputformat.class\" is not set in configuration.");
		read.validate(input);
	}

	//This test validates functionality of Read.validate() function when key class is not provided by user in configuration.
	@Test
	public void testIfKeyClassIsNotProvided() {

		Configuration conf = new Configuration();
		TypeDescriptor<DummyInputFormat> inputFormatClassName = new TypeDescriptor<DummyInputFormat>() {
			private static final long serialVersionUID = 1L;
		};
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName.getRawType(), InputFormat.class);
		TypeDescriptor<String> valueClass = new TypeDescriptor<String>() {

			private static final long serialVersionUID = 1L;
		};
		conf.setClass("value.class", valueClass.getRawType(), Object.class);
		Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withConfiguration(conf);
		PBegin input = PBegin.in(pipeline);
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Configuration property \"key.class\" is not set.");
		read.validate(input);

	}

	//This test validates functionality of Read.validate() function when value class is not provided by user in configuration.
	@Test
	public void testIfValueClassIsNotProvided() {

		Configuration conf = new Configuration();
		TypeDescriptor<DummyInputFormat> inputFormatClassName = new TypeDescriptor<DummyInputFormat>() {
			private static final long serialVersionUID = 1L;
		};

		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName.getRawType(), InputFormat.class);
		TypeDescriptor<String> keyClass = new TypeDescriptor<String>() {
			private static final long serialVersionUID = 1L;
		};
		conf.setClass("key.class", keyClass.getRawType(), Object.class);
		Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withConfiguration(conf);
		PBegin input = PBegin.in(pipeline);
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Configuration property \"value.class\" is not set.");
		read.validate(input);
	}



	//This test validates functionality of Read.validate() function when myKeyTranslate's (simple function provided by user for key translation)
	//input type is not same as hadoop input format's keyClass(Which is property set in configuration as "key.class").
	@Test
	public void testKeyTranslationFunctionIfInputTypeIsWrong() {
		Configuration conf =getConfiguration(TypeDescriptor.of(DummyInputFormat.class),TypeDescriptor.of(Text.class),TypeDescriptor.of(java.lang.String.class)).getConfiguration();
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
		thrown.expect(IllegalArgumentException.class);
		String inputFormatClassProperty= conf.get("mapreduce.job.inputformat.class") ;
		String keyClassProperty= conf.get("key.class");
		thrown.expectMessage("Key translation's input type is not same as hadoop input format : "+inputFormatClassProperty+" key class : "+ keyClassProperty);
		read.validate(input);

	}

	//This test validates functionality of Read.validate() function when myValueTranslate's (simple function provided by user for value translation)
	//input type is not same as hadoop input format's valueClass(Which is property set in configuration as "value.class").
	@Test
	public void testValueTranslationFunctionIfInputTypeIsWrong() {
		serConf = getConfiguration(TypeDescriptor.of(DummyInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(Text.class));
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
		
		thrown.expect(IllegalArgumentException.class);

		String inputFormatClassProperty= serConf.getConfiguration().get("mapreduce.job.inputformat.class") ;
		String keyClassProperty= serConf.getConfiguration().get("value.class");
		String expectedMessage="Value translation's input type is not same as hadoop input format : "+inputFormatClassProperty+" value class : "+ keyClassProperty;
		thrown.expectMessage(expectedMessage);
		read.validate(input);
	}

	@Test
	public void testReadingData()  throws Exception {
		serConf = getConfiguration(TypeDescriptor.of(DummyInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class));
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



	// This test validates behavior HadoopInputFormatSource if RecordReader object creation fails in start() method.
	@Test
	public void testReadersStartIfCreateRecordReaderFails() throws Exception {
		long inputDataSize=9,numberOfSplitsOfDummyInputFormat=3;
		serConf = getConfiguration(TypeDescriptor.of(BadCreateRecordReaderInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class)); 
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		long estimatedSize = parentHIFSource.getEstimatedSizeBytes(options);
		assertEquals(inputDataSize,estimatedSize);//
		List<BoundedSource<KV<String, String>>> boundedSourceList = (List<BoundedSource<KV<String, String>>>) parentHIFSource.splitIntoBundles(0, options);
		assertEquals(numberOfSplitsOfDummyInputFormat,boundedSourceList.size());
		for (BoundedSource<KV<String, String>> source : boundedSourceList) {
			BoundedReader<KV<String, String>> reader=source.createReader(options);
			assertEquals(new Double((float)0),reader.getFractionConsumed());
			thrown.expect(Exception.class);
			thrown.expectMessage("Exception in creating RecordReader in BadCreateRecordReaderInputFormat");
			reader.start();
			

		}

	}


	// This test validate's createReader() and start() methods.
	// This test validates behaviour of createReader() and start() method if InputFormat's getSplits() returns InputSplitList having having no records.  
	@Test
	public void testReadersCreateReaderAndStartWithZeroRecords() throws Exception {
		long inputDataSize=9,numberOfSplitsOfDummyInputFormat=3;

		serConf = getConfiguration(TypeDescriptor.of(BadRecordReaderNoRecordsInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class)); 
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		long estimatedSize = parentHIFSource.getEstimatedSizeBytes(options);
		assertEquals(inputDataSize,estimatedSize);//
		List<BoundedSource<KV<String, String>>> boundedSourceList = (List<BoundedSource<KV<String, String>>>) parentHIFSource.splitIntoBundles(0, options);
		assertEquals(numberOfSplitsOfDummyInputFormat,boundedSourceList.size());
		for (BoundedSource<KV<String, String>> source : boundedSourceList) {
			BoundedReader<KV<String, String>> reader=source.createReader(options);
			assertEquals(new Double((float)0),reader.getFractionConsumed());
			assertEquals(false,reader.start());

		}
	}
	@Test
	public void testReadersGetFractionConsumed() throws Exception {
		long inputDataSize=9,numberOfSplitsOfDummyInputFormat=3;
		serConf = getConfiguration(TypeDescriptor.of(DummyInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class));
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
			assertEquals(new Double((float)0),reader.getFractionConsumed());
			int i=0;
			boolean temp = reader.start();
			assertEquals(true,temp);
			if(temp){
				elements.add(reader.getCurrent());
				assertEquals(new Double((float)++i/numberOfSplitsOfDummyInputFormat),reader.getFractionConsumed());
				temp=reader.advance();
				assertEquals(true,temp);
				while(temp){
					assertEquals(true,temp);
					elements.add(reader.getCurrent());
					assertEquals(new Double((float)++i/numberOfSplitsOfDummyInputFormat),reader.getFractionConsumed());
					temp=reader.advance();
				}
				assertEquals(false,temp);
				bundleRecords.addAll(elements);
			}
		}
		assertThat(bundleRecords, containsInAnyOrder(referenceRecords.toArray()));
	}

	//Validate that the Reader and its parent source reads the same records.
	@Test
	public void testReaderAndParentSourceReadsSameData() throws Exception
	{
		serConf = getConfiguration(TypeDescriptor.of(DummyInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class));
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
		serConf = getConfiguration(TypeDescriptor.of(DummyInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class));
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


	//This test validates behavior of HadoopInputFormatSource.createReader() method when HadoopInputFormatSource.splitIntoBundles() is not called.
	@Test
	public void testCreateReaderIfSplitIntoBundlesNotCalled() throws Exception
	{
		serConf = getConfiguration(TypeDescriptor.of(DummyInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class));
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		thrown.expect(IOException.class);
		thrown.expectMessage("Cannot create reader as source is not split yet.");

		parentHIFSource.createReader(options);
	
	}


	//This test validates behavior of getEstimatedSizeBytes() and splitIntoBundles() when Hadoop InputFormat's getSplits() returns empty list.
	@Test
	public void testSplitIntoBundlesIfGetSplitsReturnsEmptyList() throws Exception
	{
		serConf = getConfiguration(TypeDescriptor.of(EmptyInputSplitsInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class));
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		thrown.expect(IOException.class);
		thrown.expectMessage("Cannot split the source as getSplits() is returning empty list.");
		parentHIFSource.getEstimatedSizeBytes(options);
		
		
		thrown.expect(IOException.class);
		thrown.expectMessage("Cannot split the source as getSplits() is returning empty list.");
		parentHIFSource.splitIntoBundles(0, options);


	}


	//This test validates behavior of getEstimatedSizeBytes() and splitIntoBundles() when Hadoop InputFormat's getSplits() returns NULL value.
	@Test
	public void testSplitIntoBundlesIfGetSplitsReturnsNullValue() throws Exception
	{
		serConf = getConfiguration(TypeDescriptor.of(NullInputSplitsBadInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class));
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
			thrown.expect(IOException.class);
			thrown.expectMessage("Cannot split the source as getSplits() is returning null value.");
			parentHIFSource.getEstimatedSizeBytes(options);
				
			thrown.expect(IOException.class);
			thrown.expectMessage("Cannot split the source as getSplits() is returning null value.");
			parentHIFSource.splitIntoBundles(0, options);
			
	}

	//This test validates functionality of HadoopInputFormatIO if user sets wrong key class and value class.
	@Test
	public void testHIFSourceIfUserSetsWrongKeyOrValueClass() throws Exception 
	{	
		serConf = getConfiguration(TypeDescriptor.of(MutableRecordsInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class)); 
		Coder<String> keyCoder = StringUtf8Coder.of();
		Coder<String> valueCoder = StringUtf8Coder.of();
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, keyCoder, valueCoder);
		List<BoundedSource<KV<String, String>>> boundedSourceList = parentHIFSource.splitIntoBundles(0, options);
		List<KV<String, String>> bundleRecords = new ArrayList<>();
		
			for (BoundedSource<KV<String, String>> source : boundedSourceList) {
				thrown.expect(ClassCastException.class);
				List<KV<String, String>> elems = SourceTestUtils.readFromSource(source, options);
				bundleRecords.addAll(elems);
			}
			
			//	assertTrue((ex.getMessage().startsWith("ValueClass set in configuration "+serConf.getConfiguration().get("value.class")+" is not compatible with valueClass of record reader "))
				//	|| (ex.getMessage().startsWith("KeyClass set in configuration "+serConf.getConfiguration().get("key.class")+" is not compatible with keyClass of record reader "))
				//	);
		
	}

	// This test validates records emitted in PCollection are immutable 
	// if InputFormat's recordReader returns same objects(i.e. same locations in memory) but with updated values for each record.
	@Test
	public void testImmutablityOfOutputOfReadIfRecordReaderObjectsAreMutable()  throws Exception {
		serConf = getConfiguration(TypeDescriptor.of(MutableRecordsInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(Employee.class)); 
		Coder<String> keyCoder = StringUtf8Coder.of();
		//Coder<Employee> valueCoder = AvroCoder.of(Employee.class);
		Coder<Employee> valueCoder = EmployeeCoder.of();
		//Create read to validate conf and myValueTranslateFunc.
		/*Read<String, Employee> read = HadoopInputFormatIO.<String, Employee>read()
				.withConfiguration(serConf.getConfiguration());		
		PBegin input = PBegin.in(pipeline);
		read.validate(input);
		 */
		//./valueCoder=read.getValueCoder();
		HadoopInputFormatBoundedSource<String, Employee> parentHIFSource = new HadoopInputFormatBoundedSource<String, Employee>(serConf, keyCoder, valueCoder);
		List<BoundedSource<KV<String, Employee>>> boundedSourceList = parentHIFSource.splitIntoBundles(0, options);
		List<KV<String, Employee>> bundleRecords = new ArrayList<>();
		for (BoundedSource<KV<String, Employee>> source : boundedSourceList) {
			List<KV<String, Employee>> elems = SourceTestUtils.readFromSource(source, options);
			bundleRecords.addAll(elems);
		}
		List<KV<String, String>> referenceRecords = getDummyDataOfRecordReader();
		List<KV<String, String>> transformedBundleRecords = Lists.transform(bundleRecords,new Function<KV<String, Employee>, KV<String, String>>(){
			@Override 
			public KV<String, String> apply(KV<String, Employee> input){
				return KV.of(input.getValue().getEmpID().trim(),input.getValue().getEmpName().trim());
			}
		});

		assertThat(transformedBundleRecords, containsInAnyOrder(referenceRecords.toArray()));
	}


	//This test validates records emitted in Pcollection are immutable 
	// if InputFormat's recordReader returns different objects (i.e. different locations in memory)
	@Test
	public void testImmutablityOfOutputOfReadIfRecordReaderObjectsAreImmutable()  throws Exception {

		serConf = getConfiguration(TypeDescriptor.of(ImmutableRecordsInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(Employee.class)); 
		Coder<String> keyCoder = StringUtf8Coder.of();
		//Coder<Employee> valueCoder = AvroCoder.of(Employee.class);
		Coder<Employee> valueCoder = EmployeeCoder.of();
		//Create read to validate conf and myValueTranslateFunc.
		/*Read<String, Employee> read = HadoopInputFormatIO.<String, Employee>read()
				.withConfiguration(serConf.getConfiguration());		
		PBegin input = PBegin.in(pipeline);
		read.validate(input);
		 */
		//./valueCoder=read.getValueCoder();
		HadoopInputFormatBoundedSource<String, Employee> parentHIFSource = new HadoopInputFormatBoundedSource<String, Employee>(serConf, keyCoder, valueCoder);
		List<BoundedSource<KV<String, Employee>>> boundedSourceList = parentHIFSource.splitIntoBundles(0, options);
		List<KV<String, Employee>> bundleRecords = new ArrayList<>();
		for (BoundedSource<KV<String, Employee>> source : boundedSourceList) {
			List<KV<String, Employee>> elems = SourceTestUtils.readFromSource(source, options);
			bundleRecords.addAll(elems);
		}
		List<KV<String, String>> referenceRecords = getDummyDataOfRecordReader();
		List<KV<String, String>> transformedBundleRecords = Lists.transform(bundleRecords,new Function<KV<String, Employee>, KV<String, String>>(){
			@Override 
			public KV<String, String> apply(KV<String, Employee> input){
				return KV.of(input.getValue().getEmpID().trim(),input.getValue().getEmpName().trim());
			}
		});

		assertThat(transformedBundleRecords, containsInAnyOrder(referenceRecords.toArray()));
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

	private static SerializableConfiguration getConfiguration(TypeDescriptor<?> inputFormatClassName, TypeDescriptor<?> keyClass, TypeDescriptor<?> valueClass) {

		Configuration conf = new Configuration();
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName.getRawType(), InputFormat.class);
		conf.setClass("key.class", keyClass.getRawType(), Object.class);
		conf.setClass("value.class", valueClass.getRawType(), Object.class);
		return new SerializableConfiguration(conf);
	}

	@DefaultCoder(AvroCoder.class)
	public class Employee implements Serializable{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String empID;
		private String empName;

		public Employee(String empId, String empName) {
			this.empID = empId;
			this.empName = empName;
		}

		public String getEmpID() {
			return empID;
		}
		public void setEmpID(String empID) {
			this.empID = empID;
		}
		public String getEmpName() {
			return empName;
		}
		public void setEmpName(String empName) {
			this.empName = empName;
		}
	}
}
