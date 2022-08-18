import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:

    (p | beam.Create(range(1, 11))
    # beam.CombineGlobally(sum) to return the sum of numbers from `PCollection`.
     | beam.CombineGlobally(sum)
     | LogElements())