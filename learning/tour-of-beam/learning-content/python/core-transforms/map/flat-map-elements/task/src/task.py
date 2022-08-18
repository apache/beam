import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:

    (p | beam.Create(['Apache Beam', 'Unified Batch and Streaming'])
    # Lambda function that returns a list of words from a sentence
     | beam.FlatMap(lambda sentence: sentence.split())
     | LogElements())