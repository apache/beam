import logging
from apache_beam.examples.inference import vllm_gemma_batch
from apache_beam.testing.load_tests.dataflow_cost_benchmark import DataflowCostBenchmark


class VllmGemmaBenchmarkTest(DataflowCostBenchmark):
  def __init__(self):
    self.metrics_namespace = "BeamML_vLLM"
    super().__init__(
        metrics_namespace=self.metrics_namespace,
        pcollection="WriteBQ.out0",
    )

  def test(self):
    # The perf-test framework passes --input_file, but the pipeline expects --input.
    extra_opts = {"input": self.pipeline.get_option("input_file")}

    self.result = vllm_gemma_batch.run(
        self.pipeline.get_full_options_as_args(**extra_opts),
        test_pipeline=self.pipeline)


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  VllmGemmaBenchmarkTest().run()
