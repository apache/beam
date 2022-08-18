import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:

    # List of elements
    (p | beam.Create(range(1, 11))
    # The elements filtered with the beam.Filter()
     | beam.Filter(lambda num: num % 2 == 0)
     | LogElements())

