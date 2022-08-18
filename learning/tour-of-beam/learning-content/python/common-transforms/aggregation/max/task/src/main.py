import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:

    (p | beam.Create(range(1, 11))
    # beam.combiners.Top.Largest(5) to return the larger than [5] from `PCollection`.
     | beam.combiners.Top.Largest(5)
     | LogElements())