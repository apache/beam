import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:

    (p | beam.Create([10, 20, 30, 40, 50])
    # Lambda function that returns an element by multiplying 5
     | beam.Map(lambda num: num * 5)
     | LogElements())