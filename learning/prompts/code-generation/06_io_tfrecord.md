Prompt:
Write the python code to read data from Cloud Spanner using Apache Beam.
Response:
You can read data from [TFRecord](https://www.tensorflow.org/api_docs/python/tf/data/TFRecordDataset) file using the Apache Beam [TFRecordIO](https://beam.apache.org/releases/pydoc/current/apache_beam.io.tfrecordio.html) connector which allows you to read and write data in `TFRecord` format:

```python
import logging

import apache_beam as beam
from apache_beam import Map
from apache_beam.io.tfrecordio import ReadFromTFRecord
from apache_beam.options.pipeline_options import PipelineOptions


class TFRecordOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--file_pattern",
            help="A file glob pattern to read TFRecords from."
        )

def run():
    options = TFRecordOptions()

    with beam.Pipeline(options=options) as p:

        output = (
            p
            | "Read from TFRecord" >> ReadFromTFRecord(
                file_pattern=options.file_pattern
            )
            | "Map from bytes" >> Map(map_from_bytes)
            | "Log Data" >> Map(logging.info)
        )


def map_from_bytes(element):
    """
    Deserializes the input bytes using pickle library and
    returns the reconstructed object.
    """
    # third party libraries
    import pickle

    return pickle.loads(element)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

```

`TFRecordOptions` class defines three command line arguments `file_pattern` that is used to configure `ReadFromTFRecord` transform. Those arguments are parsed from a command line using [Pipeline option pattern](https://beam.apache.org/documentation/patterns/pipeline-options/).

`ReadFromTFRecord` is a built-in Apache Beam transform that reads data from a `TFRecord` file. By default `TFRecordIO` transforms use `coders.BytesCoder()`. See [ReadFromTFRecord](https://beam.apache.org/releases/pydoc/current/apache_beam.io.tfrecordio.html#apache_beam.io.tfrecordio.ReadFromTFRecord) transform documentation for more details.


