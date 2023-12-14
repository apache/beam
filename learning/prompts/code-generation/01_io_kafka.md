Prompt:
Write the python code to read data from Apache Kafka using Apache Beam.
Response:
You can read data from [Apache Kafka](https://kafka.apache.org/) using the `ReadFromKafka` transform. The following Python code reads a topic `topic` from a Kafka server `bootstrap_servers`. The `topic` and `bootstrap_servers` are provided as a command line arguments. The data is logged to the console:

```python
import logging
import apache_beam as beam

from apache_beam import Map
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions

class KafkaOptions(PipelineOptions):
"""
Configure pipeline options for Kafka read transform.
"""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--bootstrap_servers',
            default="localhost:9092",
            help='Apache Kafka bootstrap servers'
        )
        parser.add_argument(
            '--topic',
            default="your-topic",
            help='Apache Kafka topic'
        )

#parse pipeline options
options = KafkaOptions()

with beam.Pipeline(options=options) as p:
    #this pipeline reads from a Kafka topic and logs the data to the console
    output = (p | "Read from Kafka" >> ReadFromKafka(
        consumer_config={'bootstrap.servers': options.bootstrap_servers},
        topics=[options.topic],
        with_metadata=False
        )
        | "Log Data" >> Map(logging.info))
```
For more information on how to use KafkaIO connector with Apache Beam Python SDK see [here](https://beam.apache.org/releases/pydoc/current/apache_beam.io.kafka.html).

For performance metrics of KafkaIO connector, see [KafkaIO Performance](https://beam.apache.org/performance/io/kafka/).

See [Pipeline option pattern](https://beam.apache.org/documentation/patterns/pipeline-options/) for a commmon pattern for parsing pipeline options from command line arguments.