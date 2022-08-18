import apache_beam as beam

from log_elements import LogElements

with beam.Pipeline() as p:

    # List of elements
    numbers = p | beam.Create([1, 2, 3, 4, 5])

    # Return PCollection with elements multiplied by 5
    mult5_results = numbers | beam.Map(lambda num: num * 5)

    # Return PCollection with elements multiplied by 10
    mult10_results = numbers | beam.Map(lambda num: num * 10)

    mult5_results | 'Log multiply 5' >> LogElements(prefix='Multiplied by 5: ')
    mult10_results | 'Log multiply 10' >> LogElements(prefix='Multiplied by 10: ')