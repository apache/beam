package com.dataartisans.flink.dataflow;


import java.util.Map;

import com.google.cloud.dataflow.sdk.PipelineResult;


public interface FlinkRunnerResult extends PipelineResult {
	
	Map<String, Object> getAggregators();
	
	long getNetRuntime();
}
