package com.dataartisans.flink.dataflow;

import java.util.Collections;
import java.util.Map;

public class ExecutionRunnerResult implements FlinkRunnerResult {
	
	private final Map<String, Object> aggregators;
	
	private final long runtime;
	
	
	public ExecutionRunnerResult(Map<String, Object> aggregators, long runtime) {
		this.aggregators = (aggregators == null || aggregators.isEmpty()) ?
				Collections.<String, Object>emptyMap() :
				Collections.unmodifiableMap(aggregators);
		
		this.runtime = runtime;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public Map<String, Object> getAggregators() {
		return aggregators;
	}
	
	@Override
	public long getNetRuntime() {
		return runtime;
	}
}
