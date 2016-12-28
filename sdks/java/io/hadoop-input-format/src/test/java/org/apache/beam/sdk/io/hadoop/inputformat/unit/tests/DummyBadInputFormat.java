package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

//Bad input format which returns empty list of input splits in getSplits() method
public class DummyBadInputFormat extends InputFormat {
	int numberOfRecordsInEachSplits = 3;
	int numberOfSplits = 3;

	public DummyBadInputFormat() {

	}

	@Override
	public RecordReader<String, String> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		DummyRecordReader dummyRecordReaderObj = new DummyRecordReader();
		dummyRecordReaderObj.initialize(split, context);
		return dummyRecordReaderObj;
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException {
		List<InputSplit> inputSplitList = new ArrayList();
		return inputSplitList;
	}

	public class DummyInputSplit extends InputSplit implements Writable {
		public int startIndex, endIndex;

		public DummyInputSplit() {

		}

		public DummyInputSplit(int startIndex, int endIndex) {
			this.startIndex = startIndex;
			this.endIndex = endIndex;
		}

		//returns number of records in each split
		@Override
		public long getLength() throws IOException, InterruptedException {
			return this.endIndex-this.startIndex ;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return null;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub

		}

	}

	class DummyRecordReader extends RecordReader<String, String> {

		String currentValue = null;
		int pointer = 0,recordsRead=0;
		long numberOfRecordsInSplit=0;
		HashMap<Integer, String> hmap = new HashMap<Integer, String>();

		public DummyRecordReader() {

		}

		@Override
		public void close() throws IOException {

		}

		@Override
		public String getCurrentKey() throws IOException, InterruptedException {

			return String.valueOf(pointer);
		}

		@Override
		public String getCurrentValue() throws IOException, InterruptedException {
			return hmap.get(new Integer(pointer));
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (float)recordsRead/numberOfRecordsInSplit;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext arg1) throws IOException, InterruptedException {
			/* Adding elements to HashMap */
			hmap.put(0, "Chaitanya");
			hmap.put(1, "Rahul");
			hmap.put(2, "Singh");
			hmap.put(3, "Ajeet");
			hmap.put(4, "Anuj");
			hmap.put(5, "xyz");
			hmap.put(6, "persistent");
			hmap.put(7, "apache");
			hmap.put(8, "beam");
			hmap.put(9, "beam");

			DummyInputSplit dummySplit = (DummyInputSplit) split;
			// String[] splitData=dummySplit.getLocations();
			pointer = dummySplit.startIndex - 1;
			numberOfRecordsInSplit=dummySplit.getLength();
			recordsRead = 0;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if ((recordsRead++) == numberOfRecordsInSplit)
				return false;
			pointer++;
			boolean hasNext = hmap.containsKey(pointer);

			return hasNext;
		}

	}
}