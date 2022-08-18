import apache_beam as beam

from log_elements import LogElements


def partition_fn(number, num_partitions):
    if number > 100:
        return 0
    else:
        return 1


with beam.Pipeline() as p:

    results = \
        (p | beam.Create([1, 2, 3, 4, 5, 100, 110, 150, 250])
        # Accepts PCollection and returns the PCollection array
         | beam.Partition(partition_fn, 2))

    results[0] | 'Log numbers > 100' >> LogElements(prefix='Number > 100: ')
    results[1] | 'Log numbers <= 100' >> LogElements(prefix='Number <= 100: ')