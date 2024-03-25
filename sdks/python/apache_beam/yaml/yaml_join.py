import apache_beam as beam
from apache_beam.yaml import yaml_provider


@beam.ptransform.ptransform_fn
def _SqlJoinTransform(
    pcoll, sql_transform_constructor, language=None):

  return pcoll | sql_transform_constructor('SELECT * FROM PCOLLECTION')


def create_join_providers():
  return [
      yaml_provider.SqlBackedProvider({
          'Join': _SqlJoinTransform
      }),
  ]