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
package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.HadoopInputFormatIOTest;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.HadoopInputFormatIOTest.Employee;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class EmployeeInputFormat extends InputFormat<Text, Employee> {
	int numberOfRecordsInEachSplits = 3;
	int numberOfSplits = 3;

	public EmployeeInputFormat() {

	}

	@Override
	public RecordReader<Text, Employee> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		DummyRecordReader dummyRecordReaderObj = new DummyRecordReader();
		dummyRecordReaderObj.initialize(split, context);
		return dummyRecordReaderObj;
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException {
		InputSplit dummyInputSplitObj;
		List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
		for (int i = 0; i < numberOfSplits; i++) {
			dummyInputSplitObj = new DummyInputSplit((i * numberOfSplits),
					((i * numberOfSplits) + numberOfRecordsInEachSplits));
			inputSplitList.add(dummyInputSplitObj);
		}
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
			
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
		
		}

	}

	class DummyRecordReader extends RecordReader<Text, Employee> {

		String currentValue = null;
		int pointer = 0,recordsRead=0;
		long numberOfRecordsInSplit=0;
		HashMap<Integer, Employee> hmap = new HashMap<Integer, Employee>();
		HadoopInputFormatIOTest hadoopInputFormatIOTest=new HadoopInputFormatIOTest();

		public DummyRecordReader() {

		}

		@Override
		public void close() throws IOException {

		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {

			return new Text(String.valueOf(pointer));
		}

		@Override
		public Employee getCurrentValue() throws IOException, InterruptedException {
			return hmap.get(pointer);
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (float)recordsRead/numberOfRecordsInSplit;
		}
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext arg1) throws IOException, InterruptedException {
			/* Adding elements to HashMap */
			hmap.put(0, hadoopInputFormatIOTest.new Employee ("Prabhanj", "Pune"));
			hmap.put(1, hadoopInputFormatIOTest.new Employee ("Rahul", "Pune"));
			hmap.put(2, hadoopInputFormatIOTest.new Employee ("Saikat", "Mumbai"));
			hmap.put(3, hadoopInputFormatIOTest.new Employee ("Gurumoorthy", "Hyderabad"));
			hmap.put(4, hadoopInputFormatIOTest.new Employee ("Shubham", "Banglore"));
			hmap.put(5, hadoopInputFormatIOTest.new Employee ("Neha", "Chennai"));
			hmap.put(6, hadoopInputFormatIOTest.new Employee ("Priyanka", "Pune"));
			hmap.put(7, hadoopInputFormatIOTest.new Employee ("Nikita", "Chennai"));
			hmap.put(8, hadoopInputFormatIOTest.new Employee ("Pallavi", "Pune"));
			
			DummyInputSplit dummySplit = (DummyInputSplit) split;
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