package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

import com.datastax.driver.core.Row;

public class HadoopInputFormatIOTest {

	public void Test1() {
		List<Row> CassandraData = null;
		PCollection<Row> rows = null;
		Row expectedElements = null;
		// PAssert.that(rows).contains(CassandraData);
		PAssert.that(rows).containsInAnyOrder(CassandraData);
	}

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
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName,
				InputFormat.class);
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

		HadoopInputFormatIO.Read read = HadoopInputFormatIO.read()
				.withConfiguration(conf).withKeyTranslation(myKeyTranslateFunc)
				.withValueTranslation(myValueTranslateFunc);

		assertEquals(
				"9160",
				read.getConfiguration().getConfiguration()
						.get("cassandra.input.thrift.port"));
		assertEquals("Murmur3Partitioner", read.getConfiguration()
				.getConfiguration().get("cassandra.input.partitioner.class"));
		assertEquals(myKeyTranslateFunc, read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslateFunc,
				read.getSimpleFuncForValueTranslation());
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
		Class inputFormatClassName = org.apache.cassandra.hadoop.cql3.CqlInputFormat.class;
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName,
				InputFormat.class);
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

		HadoopInputFormatIO.Read read = HadoopInputFormatIO.read()
				.withValueTranslation(myValueTranslateFunc)
				.withConfiguration(conf).withKeyTranslation(myKeyTranslateFunc);

		assertEquals(
				"9160",
				read.getConfiguration().getConfiguration()
						.get("cassandra.input.thrift.port"));
		assertEquals("Murmur3Partitioner", read.getConfiguration()
				.getConfiguration().get("cassandra.input.partitioner.class"));
		assertEquals(myKeyTranslateFunc, read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslateFunc,
				read.getSimpleFuncForValueTranslation());
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
		Class inputFormatClassName = org.apache.cassandra.hadoop.cql3.CqlInputFormat.class;
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName,
				InputFormat.class);
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

		HadoopInputFormatIO.Read read = HadoopInputFormatIO.read()
				.withValueTranslation(myValueTranslateFunc)
				.withConfiguration(conf).withKeyTranslation(myKeyTranslateFunc);

		DisplayData displayData = DisplayData.from(read);
		if (conf != null) {
			Iterator<Entry<String, String>> propertyElement = conf.iterator();
			while (propertyElement.hasNext()) {
				Entry<String, String> element = propertyElement.next();

				// need method hasDisplayItem
				/*assertThat(displayData,
						hasDisplayItem(element.getKey(), element.getValue()));*/
			}
		}

	}

	@Test
	public void testReaderGetFractionConsumed() {

	}

	class DummyInputFormat extends InputFormat {
		@Override
		public RecordReader createRecordReader(InputSplit arg0,
				TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			DummyRecordReader dummyRecordReaderObj = new DummyRecordReader();
			return dummyRecordReaderObj;
		}

		@Override
		public List<InputSplit> getSplits(JobContext arg0) throws IOException,
				InterruptedException {
			InputSplit dummyInputSplitObj = new DummyInputSplit();
			List<InputSplit> inputSplitList = new ArrayList();
			inputSplitList.add(dummyInputSplitObj);
			return inputSplitList;
		}

	}

	class DummyInputSplit extends InputSplit {

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return null;
		}

	}

	class DummyRecordReader extends RecordReader {

		String currentValue = null;
		int pointer = 0;
		HashMap<Integer, String> hmap = new HashMap<Integer, String>();

		@Override
		public void close() throws IOException {

		}

		@Override
		public Object getCurrentKey() throws IOException, InterruptedException {

			return null;
		}

		@Override
		public Object getCurrentValue() throws IOException,
				InterruptedException {
			return null;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
			/* Adding elements to HashMap */
			hmap.put(12, "Chaitanya");
			hmap.put(2, "Rahul");
			hmap.put(7, "Singh");
			hmap.put(49, "Ajeet");
			hmap.put(3, "Anuj");
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return false;
		}

	}
}
