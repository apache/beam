import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to

with beam.Pipeline() as p:
    output_titles = (
        p
        | "Create input" >> beam.Create([(0,0)])
        | "Batch in groups" >> beam.GroupIntoBatches(5)
        | beam.Reshuffle()
        )
    output_titles | beam.Map(print)
    assert_that(output_titles, equal_to([(0, (0,))]))
