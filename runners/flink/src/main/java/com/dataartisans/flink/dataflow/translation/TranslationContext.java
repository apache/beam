package com.dataartisans.flink.dataflow.translation;

import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.values.PValue;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class TranslationContext {
	
	private final Map<PValue, DataSet<?>> dataSets;
	
	private final ExecutionEnvironment env;
	
	// ------------------------------------------------------------------------
	
	public TranslationContext(ExecutionEnvironment env) {
		this.env = env;
		this.dataSets = new HashMap<>();
	}
	
	// ------------------------------------------------------------------------
	
	public ExecutionEnvironment getExecutionEnvironment() {
		return env;
	}
	
//	private <T> DataSet<T> getDataSet(TransformTreeNode node) {
//		@SuppressWarnings("unchecked")
//		DataSet<T> typedSet = (DataSet<T>) dataSets.get(node);
//		return typedSet;
//	}
	
	public DataSet<?> getInputDataSet(TransformTreeNode node) {
		PValue value = (PValue) node.getInput();
		return dataSets.get(value);
	}
	
	public void setOutputDataSet(TransformTreeNode node, DataSet<?> value) {
		PValue output = (PValue) node.getOutput();
		dataSets.put(output, value);
	}
	
//	public void registerDataSet(DataSet<?> dataSet, TransformTreeNode node) {
//		DataSet<?> previous = dataSets.put(node, dataSet);
//
//		if (previous != null) {
//			// undo the action
//			dataSets.put(node, previous);
//			throw new IllegalArgumentException(
//					"Context contains already a DataSet as the result of the given TreeTransformNode.");
//		}
//	}
}
