"""Tests for google.cloud.dataflow.worker.worker_runner."""

import logging
import unittest

import apache_beam as beam
from apache_beam.runners.portability import fn_api_runner
from apache_beam.runners.portability import maptask_executor_runner


class FnApiRunnerTest(maptask_executor_runner.MapTaskExecutorRunner):

  def create_pipeline(self):
    return beam.Pipeline(runner=fn_api_runner.FnApiRunner())

  def test_combine_per_key(self):
    # TODO(robertwb): Implement PGBKCV operation.
    pass

  # Inherits all tests from worker_runner_base_test.WorkerRunnerBaseTest


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
