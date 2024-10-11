import argparse
from datetime import datetime, date
import logging
import random
import json
import typing

from typing import Tuple, List, Any, Iterable


import apache_beam as beam
from apache_beam.io.kafka import WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import Pipeline
import apache_beam.typehints.schemas 
from apache_beam.typehints.schemas  import Any as BeamAny

DATA = [
        ('user1', 1),
        ('user2', 2),
    ]

def prepare_for_kafka(element:Tuple['str', 'int']) :
    key = 'test-key'
    return (key.encode('utf8'), json.dumps(element).encode('utf8'))

def run():
    """
    main entry point to run Apache Beam Job

    """

    with Pipeline() as pipeline:
        lines = (pipeline | 'Create data' >> beam.Create(DATA) 
        | "Prepare " >> beam.Map(prepare_for_kafka).with_output_types(typing.Tuple[bytes, bytes])
        | "Write to Kafka" >>  WriteToKafka(
                producer_config={'bootstrap.servers': "localhost:9092"},
                    topic='test-out',
                 )
                 )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    run()
