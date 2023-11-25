Prompt:
Write the python code to read messages from a PubSub subscription.
Response:
You can read messages from a PubSub subsription or topic using the `ReadFromPubSub` transform. PubSub is currently supported only in streaming pipelines.

The following python code reads messages from a PubSub subscription. The subscription is provided as a command line argument. The messages are logged to the console:

```python
import logging

import apache_beam as beam
from apache_beam import Map
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions


class PubSubReadOptions(PipelineOptions):
"""
Configure pipeline options for PubSub read transform.
"""
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument(
          "--subscription",
          required=True,
          help="PubSub subscription to read from.")

def read_subscription():
  """Read from PubSub subscription function."""

  #parse pipeline options
  #streaming=True is required for a streaming pipeline
  options = PubSubReadOptions(streaming=True)
  
  with beam.Pipeline(options=options) as p:
    #this pipeline reads from a PubSub subscription and logs the messages to the console
    (p | "Read PubSub subscription" >> ReadFromPubSub(subscription=options.subscription)
       | "Format message" >> Map(lambda message: f"Received message:\n{message}\n")
       | Map(logging.info))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  read_subscription()

```
Reading messages directly from a topic is also supported. A temporary subscription will be created automatically.

The messages could be returned as a byte string or as PubsubMessage objects. This behavior is controlled by the `with_attributes` parameter.

See [PubSub IO](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html) transform documentation for more details.

For a common pattern for configuring pipeline options see here [Pipeline option pattern](https://beam.apache.org/documentation/patterns/pipeline-options/).

