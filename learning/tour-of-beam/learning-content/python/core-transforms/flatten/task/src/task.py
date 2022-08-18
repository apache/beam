import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:

    # List of elements start with a
    wordsStartingWithA = \
        p | 'Words starting with A' >> beam.Create(['apple', 'ant', 'arrow'])

    # List of elements start with b
    wordsStartingWithB = \
        p | 'Words starting with B' >> beam.Create(['ball', 'book', 'bow'])

    ((wordsStartingWithA, wordsStartingWithB)
    # Accept two PCollection data types are the same combines and returns one PCollection
     | beam.Flatten()
     | LogElements())