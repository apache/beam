from collections import namedtuple

import apache_beam as beam
from apache_beam import named_tuple_to_schema, RowCoder


def run():
  r = beam.Row(id=1, value='integer')
  with beam.Pipeline() as p:
    pcol = p | beam.Create([r])
  schema = named_tuple_to_schema(pcol.element_type)
  rc = RowCoder(schema)
  print(schema)
  print(rc.encode(r))
  # Schema()

  print('=' * 20)

  d = {'id': 1, 'value': 'name'}
  br = beam.Row(**d)
  print(type(br))
  nt = namedtuple('x', d.keys())(*d.values())
  print(nt)
  rt = named_tuple_to_schema(nt)
  print(rt)


if __name__ == '__main__':
  run()
