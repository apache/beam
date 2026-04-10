import logging

from apache_beam.utils.timestamp import Timestamp
from apache_beam.transforms.util import LogElements
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.periodicsequence import RebaseMode
from apache_beam.options.pipeline_options import PipelineOptions

# prism runner option
prism_options = PipelineOptions([
    "--streaming",
    "--environment_type=LOOPBACK",
    "--runner=PrismRunner",
])

# dataflow runner option
# run `python -m build --sdist` to build the source tarball first.
dataflow_options = PipelineOptions([
    "--streaming",
    "--runner=DataflowRunner",
    "--temp_location=gs://apache-beam-testing-timestamp-buffer/temp",
    "--staging_location=gs://apache-beam-testing-timestamp-buffer/staging",
    "--project=apache-beam-testing",
    "--region=us-central1",
    "--sdk_location=dist/apache_beam-2.74.0.dev0.tar.gz",
])

now = Timestamp.now()
data = list([(0, i) for i in range(100)])
periodic_source = PeriodicImpulse(
          start_timestamp=now,
          stop_timestamp=now + 30,
          data=data,
          fire_interval=0.1,
          rebase=RebaseMode.REBASE_ALL)

dump_to_log = LogElements(
          level=logging.WARNING,
          with_timestamp=True,
          with_window=True,
          with_pane_info=True,
          use_epoch_time=True)
