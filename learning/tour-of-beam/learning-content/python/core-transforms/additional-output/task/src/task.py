import apache_beam as beam
from apache_beam import pvalue

from log_elements import LogElements

num_below_100_tag = 'num_below_100'
num_above_100_tag = 'num_above_100'


class ProcessNumbersDoFn(beam.DoFn):

    def process(self, element):
        if element <= 100:
            yield element
        else:
            yield pvalue.TaggedOutput(num_above_100_tag, element)


with beam.Pipeline() as p:

    results = \
        (p | beam.Create([10, 50, 120, 20, 200, 0])
         | beam.ParDo(ProcessNumbersDoFn())
         .with_outputs(num_above_100_tag, main=num_below_100_tag))

    # First PCollection
    results[num_below_100_tag] | 'Log numbers <= 100' >> LogElements(prefix='Number <= 100: ')

    # Additional PCollection
    results[num_above_100_tag] | 'Log numbers > 100' >> LogElements(prefix='Number > 100: ')