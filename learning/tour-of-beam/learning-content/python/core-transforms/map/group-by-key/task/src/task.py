import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:

    (p | beam.Create(['apple', 'ball', 'car', 'bear', 'cheetah', 'ant'])
    # Returns a map which key will be the first letter, and the values are a list of words
     | beam.Map(lambda word: (word[0], word))
     | beam.GroupByKey()
     | LogElements())