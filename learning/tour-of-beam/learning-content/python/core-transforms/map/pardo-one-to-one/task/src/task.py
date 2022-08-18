import apache_beam as beam

from log_elements import LogElements

# Multiplications by 10
class MultiplyByTenDoFn(beam.DoFn):

    def process(self, element):
        yield element * 10


with beam.Pipeline() as p:

    (p | beam.Create([1, 2, 3, 4, 5])
    # Transform simple DoFn operation
     | beam.ParDo(MultiplyByTenDoFn())
     | LogElements())