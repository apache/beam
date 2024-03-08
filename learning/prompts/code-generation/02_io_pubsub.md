Prompt:
Write the Python code to read messages from a Pub/Sub subscription.

Response:
Your Apache Beam pipeline can read messages from a Pub/Sub subscription or topic using the `ReadFromPubSub` transform. Pub/Sub is only supported in streaming pipelines.

The following Python code reads messages from a Pub/Sub subscription and logs the data to the console. The subscription is provided as a command line argument:

```python
import logging

import apache_beam as beam
from apache_beam import Map
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions

class PubSubReadOptions(PipelineOptions):
"""Configure pipeline options for Pub/Sub read transform."""

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          "--subscription",
          required=True,
          help="Pub/Sub subscription to read from.")

def read_subscription():
  """Read from Pub/Sub subscription function."""

  # Parse the pipeline options.
  # For streaming pipelines, you need to include streaming=True
  options = PubSubReadOptions(streaming=True)

  with beam.Pipeline(options=options) as p:
    # This pipeline reads from a Pub/Sub subscription and logs the messages to the console.
    (p | "Read Pub/Sub subscription" >> ReadFromPubSub(subscription=options.subscription)
       | "Format message" >> Map(lambda message: f"Received message:\n{message}\n")
       | Map(logging.info))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  read_subscription()

```

Apache Beam also supports reading messages directly from a topic. A temporary subscription will be created automatically.

The messages could be returned as a byte string or as `PubsubMessage` objects. This behavior is controlled by the `with_attributes` parameter.

For more information, see the [Pub/Sub I/O transform documentation](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html).

For a common pattern for configuring pipeline options, see [Pipeline option patterns](https://beam.apache.org/documentation/patterns/pipeline-options/).
