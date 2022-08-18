import apache_beam as beam

from log_elements import LogElements


class ExtractAndMultiplyNumbers(beam.PTransform):

    def expand(self, pcoll):
        return (pcoll
                # First operation
                | beam.FlatMap(lambda line: map(int, line.split(',')))
                # Second operation
                | beam.Map(lambda num: num * 10)
                )


with beam.Pipeline() as p:

    (p | beam.Create(['1,2,3,4,5', '6,7,8,9,10'])
     | ExtractAndMultiplyNumbers()
     | LogElements())