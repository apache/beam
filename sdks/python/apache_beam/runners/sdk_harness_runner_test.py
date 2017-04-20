"""Tests for google.cloud.dataflow.worker.worker_runner."""

import logging
import unittest

import google3

import apache_beam as beam
from dataflow_worker import worker_runner_base_test
from dataflow_worker.fn_harness import sdk_harness_runner
import faulthandler


class SdkHarnessRunnerTest(worker_runner_base_test.WorkerRunnerBaseTest):

  def create_pipeline(self):
    return beam.Pipeline(runner=sdk_harness_runner.SdkHarnessRunner())

  def test_combine_per_key(self):
    # TODO(robertwb): Implement PGBKCV operation.
    pass

  # Inherits all tests from worker_runner_base_test.WorkerRunnerBaseTest


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  faulthandler.enable()
  unittest.main()
