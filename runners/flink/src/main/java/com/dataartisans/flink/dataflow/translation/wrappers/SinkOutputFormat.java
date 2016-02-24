/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.dataflow.translation.wrappers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.common.base.Preconditions;
import com.google.cloud.dataflow.sdk.transforms.Write;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.AbstractID;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;

/**
 * Wrapper class to use generic Write.Bound transforms as sinks.
 * @param <T> The type of the incoming records.
 */
public class SinkOutputFormat<T> implements OutputFormat<T> {

	private final Sink<T> sink;

	private transient PipelineOptions pipelineOptions;

	private Sink.WriteOperation<T, ?> writeOperation;
	private Sink.Writer<T, ?> writer;

	private AbstractID uid = new AbstractID();

	public SinkOutputFormat(Write.Bound<T> transform, PipelineOptions pipelineOptions) {
		this.sink = extractSink(transform);
		this.pipelineOptions = Preconditions.checkNotNull(pipelineOptions);
	}

	private Sink<T> extractSink(Write.Bound<T> transform) {
		// TODO possibly add a getter in the upstream
		try {
			Field sinkField = transform.getClass().getDeclaredField("sink");
			sinkField.setAccessible(true);
			@SuppressWarnings("unchecked")
			Sink<T> extractedSink = (Sink<T>) sinkField.get(transform);
			return extractedSink;
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException("Could not acquire custom sink field.", e);
		}
	}

	@Override
	public void configure(Configuration configuration) {
		writeOperation = sink.createWriteOperation(pipelineOptions);
		try {
			writeOperation.initialize(pipelineOptions);
		} catch (Exception e) {
			throw new RuntimeException("Failed to initialize the write operation.", e);
		}
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		try {
			writer = writeOperation.createWriter(pipelineOptions);
		} catch (Exception e) {
			throw new IOException("Couldn't create writer.", e);
		}
		try {
			writer.open(uid + "-" + String.valueOf(taskNumber));
		} catch (Exception e) {
			throw new IOException("Couldn't open writer.", e);
		}
	}

	@Override
	public void writeRecord(T record) throws IOException {
		try {
			writer.write(record);
		} catch (Exception e) {
			throw new IOException("Couldn't write record.", e);
		}
	}

	@Override
	public void close() throws IOException {
		try {
			writer.close();
		} catch (Exception e) {
			throw new IOException("Couldn't close writer.", e);
		}
	}

	private void writeObject(ObjectOutputStream out) throws IOException, ClassNotFoundException {
		out.defaultWriteObject();
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(out, pipelineOptions);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		ObjectMapper mapper = new ObjectMapper();
		pipelineOptions = mapper.readValue(in, PipelineOptions.class);
	}
}
