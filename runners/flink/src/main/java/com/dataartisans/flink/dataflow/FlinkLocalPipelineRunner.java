package com.dataartisans.flink.dataflow;

import com.dataartisans.flink.dataflow.translation.FlinkTranslator;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsValidator;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class FlinkLocalPipelineRunner extends PipelineRunner<FlinkRunnerResult> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkLocalPipelineRunner.class);
	
	
	// --------------------------------------------------------------------------------------------
	// Run methods
	// --------------------------------------------------------------------------------------------
	
	@Override
	public FlinkRunnerResult run(Pipeline pipeline) {
		return run(pipeline, -1);
	}
	
	public FlinkRunnerResult run(Pipeline pipeline, int parallelism) {
		if (parallelism <= 0 && parallelism != -1) {
			throw new IllegalArgumentException("Parallelism must be positive or -1 for default");
		}
		
		LOG.info("Executing pipeline using the FlinkLocalPipelineRunner.");
		
//		ExecutionEnvironment env = parallelism == -1 ?
//				ExecutionEnvironment.createLocalEnvironment() :
//				ExecutionEnvironment.createLocalEnvironment(parallelism);
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		LOG.info("Translating pipeline to Flink program.");
		
		FlinkTranslator translator = new FlinkTranslator(env);
		translator.translate(pipeline);
		
		LOG.info("Starting execution of Flink program.");
		
		JobExecutionResult result;
		try {
			env.setDegreeOfParallelism(1);
//			System.out.println(env.getExecutionPlan());
			result = env.execute();
		}
		catch (Exception e) {
			LOG.error("Pipeline execution failed", e);
			throw new RuntimeException("Pipeline execution failed", e);
		}
		
		LOG.info("Execution finished in {} msecs", result.getNetRuntime());
		
		Map<String, Object> accumulators = result.getAllAccumulatorResults();
		if (accumulators != null && !accumulators.isEmpty()) {
			LOG.info("Final aggregator values:");
			
			for (Map.Entry<String, Object> entry : result.getAllAccumulatorResults().entrySet()) {
				LOG.info("{} : {}", entry.getKey(), entry.getValue());
			}
		}

		return new ExecutionRunnerResult(accumulators, result.getNetRuntime());
	}
	
	/**
	 * Constructs a DirectPipelineRunner from the given options.
	 */
	public static FlinkLocalPipelineRunner fromOptions(PipelineOptions options) {
		FlinkPipelineOptions directOptions =
				PipelineOptionsValidator.validate(FlinkPipelineOptions.class, options);
		LOG.debug("Creating FlinkPipelineRunner");
		return new FlinkLocalPipelineRunner(directOptions);
	}

	private final FlinkPipelineOptions options;

	/** Returns a new DirectPipelineRunner. */
	private FlinkLocalPipelineRunner(FlinkPipelineOptions options) {
		this.options = options;
	}

	public FlinkPipelineOptions getPipelineOptions() {
		return options;
	}

	@Override
	public String toString() { return "FlinkLocalPipelineRunner#" + hashCode(); }

	/**
	 * Constructs a runner with default properties for testing.
	 *
	 * @return The newly created runner.
	 */
	public static FlinkLocalPipelineRunner createForTest() {
		FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
		options.setGcpCredential(new TestCredential());
		return new FlinkLocalPipelineRunner(options);
	}
}
