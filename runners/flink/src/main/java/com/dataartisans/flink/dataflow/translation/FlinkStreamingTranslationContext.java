/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.translation;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class FlinkStreamingTranslationContext {

	private final StreamExecutionEnvironment env;
	private final PipelineOptions options;

	/**
	 * Keeps a mapping between the output value of the PTransform (in Dataflow) and the
	 * Flink Operator that produced it, after the translation of the correspondinf PTransform
	 * to its Flink equivalent.
	 * */
	private final Map<PValue, DataStream<?>> dataStreams;

	private AppliedPTransform<?, ?, ?> currentTransform;

	public FlinkStreamingTranslationContext(StreamExecutionEnvironment env, PipelineOptions options) {
		this.env = env;
		this.options = options;
		this.dataStreams = new HashMap<>();
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return env;
	}

	public PipelineOptions getPipelineOptions() {
		return options;
	}

	@SuppressWarnings("unchecked")
	public <T> DataStream<T> getInputDataStream(PValue value) {
		return (DataStream<T>) dataStreams.get(value);
	}

	public void setOutputDataStream(PValue value, DataStream<?> set) {
		if (!dataStreams.containsKey(value)) {
			dataStreams.put(value, set);
		}
	}

	/**
	 * Sets the AppliedPTransform which carries input/output.
	 * @param currentTransform
	 */
	public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
		this.currentTransform = currentTransform;
	}

	@SuppressWarnings("unchecked")
	public <I extends PInput> I getInput(PTransform<I, ?> transform) {
		return (I) currentTransform.getInput();
	}

	@SuppressWarnings("unchecked")
	public <O extends POutput> O getOutput(PTransform<?, O> transform) {
		return (O) currentTransform.getOutput();
	}
}
