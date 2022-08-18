import apache_beam as beam

from log_elements import LogElements

# DoFn with tokenize function
class BreakIntoWordsDoFn(beam.DoFn):

    def process(self, element):
        return element.split()


with beam.Pipeline() as p:

    (p | beam.Create(['Hello Beam', 'It is awesome'])
     # Transform with tokenize DoFn operation
     | beam.ParDo(BreakIntoWordsDoFn())
     | LogElements())