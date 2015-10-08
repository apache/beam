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
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TypedPValue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

import java.util.HashMap;
import java.util.Map;

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
}
