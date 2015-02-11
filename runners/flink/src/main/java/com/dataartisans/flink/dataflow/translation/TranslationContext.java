package com.dataartisans.flink.dataflow.translation;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;

public class TranslationContext {
	
	private final Map<TransformTreeNode, DataSet<?>> dataSets;
	
	private final ExecutionEnvironment env;
	
	// ------------------------------------------------------------------------
	
	public TranslationContext(ExecutionEnvironment env) {
		this.env = env;
		this.dataSets = new HashMap<TransformTreeNode, DataSet<?>>();
	}
	
	// ------------------------------------------------------------------------
	
	public ExecutionEnvironment getExecutionEnvironment() {
		return env;
	}
	
	public <T> DataSet<T> getDataSet(TransformTreeNode node) {
		@SuppressWarnings("unchecked")
		DataSet<T> typedSet = (DataSet<T>) dataSets.get(node);
		return typedSet;
	}
	
	public void registerDataSet(DataSet<?> dataSet, TransformTreeNode node) {
		DataSet<?> previous = dataSets.put(node, dataSet);
		
		if (previous != null) {
			// undo the action
			dataSets.put(node, previous);
			throw new IllegalArgumentException(
					"Context contains already a DataSet as the result of the given TreeTransformNode.");
		}
	}
}
