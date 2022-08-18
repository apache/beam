import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:

    (p | beam.Create(['apple', 'banana', 'cherry', 'durian', 'guava', 'melon'])
    # The WithKeys return Map which key will be first letter word and value word
    | beam.WithKeys(lambda word: word[0:1])
     | LogElements())