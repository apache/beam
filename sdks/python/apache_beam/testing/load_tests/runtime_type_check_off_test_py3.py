import logging

from .runtime_type_check.base_runtime_type_check import BaseRunTimeTypeCheckTest

"""
For more information, see documentation in the parent class: BaseRunTimeTypeCheckTest.

Example test run:

python -m apache_beam.testing.load_tests.runtime_type_check_off_test_py3 \
    --test-pipeline-options="
    --project=apache-beam-testing
    --region=us-centrall
    --publish_to_big_query=true
    --metrics_dataset=python_load_tests
    --metrics_table=gbk
    --nested_typehint=0
    --fanout=200
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\": 15
    }'"
"""


class RunTimeTypeCheckOffTest(BaseRunTimeTypeCheckTest):
  def __init__(self):
    self.runtime_type_check = False
    super(RunTimeTypeCheckOffTest, self).__init__()


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  RunTimeTypeCheckOffTest().run()
