import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:

    (p | beam.Create(range(1, 11))
    # beam.combiners.Count.Globally() to return the count of numbers from `PCollection`.
     | beam.combiners.Count.Globally()
     | LogElements())