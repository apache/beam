import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:

    (p | beam.Create(range(1, 11))
    # beam.combiners.Mean.Globally() to return the mean of numbers from `PCollection`.
     | beam.combiners.Mean.Globally()
     | LogElements())