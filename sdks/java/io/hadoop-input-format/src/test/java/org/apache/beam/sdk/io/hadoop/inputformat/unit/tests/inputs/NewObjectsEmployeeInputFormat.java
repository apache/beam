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
package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * <p>
 * This is a valid InputFormat for reading employee data which is available in
 * the form of {@code List<KV>} as
 * {@linkplain NewObjectsEmployeeRecordReader#employeeDataList employeeDataList}. {@linkplain NewObjectsEmployeeRecordReader#employeeDataList
 * employeeDataList} is populated using
 * {@linkplain TestEmployeeDataSet#populateEmployeeData()}.
 * <p>
 * {@linkplain NewObjectsEmployeeInputFormat} splits data into
 * {@value TestEmployeeDataSet#NUMBER_OF_SPLITS} splits, each split having
 * {@value TestEmployeeDataSet#NUMBER_OF_RECORDS_IN_EACH_SPLIT} records each.
 * {@linkplain NewObjectsEmployeeInputFormat} reads data from
 * {@linkplain NewObjectsEmployeeRecordReader#employeeDataList employeeDataList}
 * and produces a key (employee id) of type Text and value of type
 * {@linkplain Employee Employee}.
 * <p>
 * {@linkplain NewObjectsEmployeeInputFormat} is also given as input to test
 * whether {@linkplain HadoopInputFormatIO } source returns immutable records for
 * a scenario when RecordReader creates new key and value objects every time it
 * reads data.
 */
public class NewObjectsEmployeeInputFormat extends InputFormat<Text, Employee> {

	public NewObjectsEmployeeInputFormat() {
	}

	@Override
	public RecordReader<Text, Employee> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new NewObjectsEmployeeRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException,
			InterruptedException {
		List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
		for (int i = 1; i <= TestEmployeeDataSet.NUMBER_OF_SPLITS; i++) {
			InputSplit inputSplitObj = new NewObjectsEmployeeInputSplit(
					((i - 1) * TestEmployeeDataSet.NUMBER_OF_RECORDS_IN_EACH_SPLIT),
					(i * TestEmployeeDataSet.NUMBER_OF_RECORDS_IN_EACH_SPLIT - 1));
			inputSplitList.add(inputSplitObj);
		}
		return inputSplitList;
	}

	public static class NewObjectsEmployeeInputSplit extends InputSplit implements
			Writable {
		// Start and end map index of each split of employeeData.
		private long startIndex;
		private long endIndex;

		public NewObjectsEmployeeInputSplit() {
		}

		public NewObjectsEmployeeInputSplit(long startIndex, long endIndex) {
			this.startIndex = startIndex;
			this.endIndex = endIndex;
		}

		/**
		 * Returns number of records in each split.
		 */
		@Override
		public long getLength() throws IOException, InterruptedException {
			return this.endIndex - this.startIndex + 1;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return null;
		}

		public long getStartIndex() {
			return startIndex;
		}

		public long getEndIndex() {
			return endIndex;
		}

		@Override
		public void readFields(DataInput dataIn) throws IOException {
			startIndex = dataIn.readLong();
			endIndex = dataIn.readLong();
		}

		@Override
		public void write(DataOutput dataOut) throws IOException {
			dataOut.writeLong(startIndex);
			dataOut.writeLong(endIndex);
		}
	}

	public class NewObjectsEmployeeRecordReader extends
			RecordReader<Text, Employee> {

		private NewObjectsEmployeeInputSplit split;
		private Text currentKey;
		private Employee currentValue;
		private long employeeListIndex = 0L;
		private long recordsRead = 0L;
		private List<KV<String, String>> employeeDataList;

		public NewObjectsEmployeeRecordReader() {
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return currentKey;
		}

		@Override
		public Employee getCurrentValue() throws IOException,
				InterruptedException {
			return currentValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (float) recordsRead / split.getLength();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
			this.split = (NewObjectsEmployeeInputSplit) split;
			employeeListIndex = this.split.getStartIndex() - 1;
			recordsRead = 0;
			employeeDataList = TestEmployeeDataSet.populateEmployeeData();
			currentValue = new Employee(null, null);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if ((recordsRead++) >= split.getLength()) {
				return false;
			}
			employeeListIndex++;
			KV<String, String> employeeDetails = employeeDataList
					.get((int) employeeListIndex);
			String empData[] = employeeDetails.getValue().split("_");
			/*
			 * New objects returned every time for key and value to signify the same
			 * object's state is not changed and returned instead every time a new object is returned.
			 */
			currentKey = new Text(employeeDetails.getKey());
			currentValue = new Employee(empData[0], empData[1]);
			return true;
		}
	}
}
