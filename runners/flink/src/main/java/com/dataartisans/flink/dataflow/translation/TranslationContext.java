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
package com.dataartisans.flink.dataflow.translation;

import com.dataartisans.flink.dataflow.translation.types.CoderTypeInformation;
import com.dataartisans.flink.dataflow.translation.types.KvCoderTypeInformation;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TypedPValue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class TranslationContext {
	
	private final Map<PValue, DataSet<?>> dataSets;
	private final Map<PCollectionView<?>, DataSet<?>> broadcastDataSets;

	private final ExecutionEnvironment env;
	private final PipelineOptions options;

	private AppliedPTransform<?, ?, ?> currentTransform;
	
	// ------------------------------------------------------------------------
	
	public TranslationContext(ExecutionEnvironment env, PipelineOptions options) {
		this.env = env;
		this.options = options;
		this.dataSets = new HashMap<>();
		this.broadcastDataSets = new HashMap<>();
	}
	
	// ------------------------------------------------------------------------
	
	public ExecutionEnvironment getExecutionEnvironment() {
		return env;
	}

	public PipelineOptions getPipelineOptions() {
		return options;
	}
	
	@SuppressWarnings("unchecked")
	public <T> DataSet<T> getInputDataSet(PValue value) {
		return (DataSet<T>) dataSets.get(value);
	}

	public <T> DataSet<T> getCurrentInputDataSet() {
		return getInputDataSet(getCurrentInput());
	}
	
	public void setOutputDataSet(PValue value, DataSet<?> set) {
		if (!dataSets.containsKey(value)) {
			dataSets.put(value, set);
		}
	}

	/**
	 * Gets the applied AppliedPTransform which carries input/output.
	 * @return
	 */
	public AppliedPTransform<?, ?, ?> getCurrentTransform() {
		return currentTransform;
	}

	/**
	 * Sets the AppliedPTransform which carries input/output.
	 * @param currentTransform
	 */
	public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
		this.currentTransform = currentTransform;
	}

	@SuppressWarnings("unchecked")
	public <T> DataSet<T> getSideInputDataSet(PCollectionView<?> value) {
		return (DataSet<T>) broadcastDataSets.get(value);
	}

	public void setSideInputDataSet(PCollectionView<?> value, DataSet<?> set) {
		if (!broadcastDataSets.containsKey(value)) {
			broadcastDataSets.put(value, set);
		}
	}
	
	@SuppressWarnings("unchecked")
	public <T> TypeInformation<T> getTypeInfo(PValue output) {
		if (output instanceof TypedPValue) {
			Coder<?> outputCoder = ((TypedPValue) output).getCoder();
			if (outputCoder instanceof KvCoder) {
				return new KvCoderTypeInformation((KvCoder) outputCoder);
			} else {
				return new CoderTypeInformation(outputCoder);
			}
		}
		return new GenericTypeInfo<T>((Class<T>)Object.class);
	}

	public <T> TypeInformation<T> getCurrentInputTypeInfo() {
		return getTypeInfo((PValue) currentTransform.getInput());
	}

	public <T> TypeInformation<T> getCurrentOutputTypeInfo() {
		return getTypeInfo((PValue) currentTransform.getOutput());
	}

	public PValue getCurrentInput() {
		return (PValue) currentTransform.getInput();
	}

	public PValue getCurrentOutput() {
		return (PValue) currentTransform.getOutput();
	}

	@SuppressWarnings("unchecked")
	<I extends PInput> I getInput(PTransform<I, ?> transform) {
		I input = (I) currentTransform.getInput();
		return input;
	}

	@SuppressWarnings("unchecked")
	<O extends POutput> O getOutput(PTransform<?, O> transform) {
		O output = (O) currentTransform.getOutput();
		return output;
	}
}
