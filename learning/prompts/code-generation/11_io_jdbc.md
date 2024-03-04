Prompt:
Write the Python code to read data from Oracle JDBC using Apache Beam.

Response:
You can read data from an Oracle Java Database Connectivity (JDBC) enabled database using the Apache Beam [JdbcIO connector](https://beam.apache.org/releases/pydoc/current/apache_beam.io.jdbc.html).
```python
import logging
import apache_beam as beam
import typing

from apache_beam import coders
from apache_beam import Map
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.options.pipeline_options import PipelineOptions
from typing import NamedTuple


class ExampleRow(NamedTuple):
    id: int
    name: str


class JdbcOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--table_name',
            help='Table name'
        )
        parser.add_argument(
            '--jdbc_url',
            help='JDBC URL'
        )
        parser.add_argument(
            '--driver_class_name',
            help='Driver class name'
        )
        parser.add_argument(
            '--username',
            help='Username'
        )
        parser.add_argument(
            '--password',
            help='Password'
        )


def run():
    """
    This pipeline shows how to read from JDBC.
    """

    options = JdbcOptions()
    coders.registry.register_coder(ExampleRow, coders.RowCoder)

    with beam.Pipeline(options=options) as p:

        output = (p | 'Read from JDBC' >> ReadFromJdbc(
                      table_name=options.table_name,
                      driver_class_name=options.driver_class_name,
                      jdbc_url=options.jdbc_url,
                      username=options.username,
                      password=options.password
                    )
                    | 'Log Data' >> Map(logging.info))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
```

The `JdbcOptions` class is a custom class that extends the `PipelineOptions` object. It is used to parse pipeline options from command-line arguments. This code uses [pipeline options](https://beam.apache.org/documentation/patterns/pipeline-options/).

A Beam pipeline is created using the `JdbcOptions` class and the [ReadFromJdbc transform](https://beam.apache.org/releases/pydoc/current/apache_beam.io.jdbc.html#apache_beam.io.jdbc.ReadFromJdbc) is used to read data from the database via JDBC.
