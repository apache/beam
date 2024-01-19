import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.typehints.row_type import RowTypeConstraint
import typing
import subprocess
import os
import warnings
import sys
from apache_beam.transforms.external_schematransform_provider import ExternalSchemaTransformProvider
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.typehints.schemas import named_tuple_to_schema
from apache_beam.typehints.schemas import named_tuple_from_schema

if __name__ == '__main__':
  row = beam.Row(num=1, str="a")
  nt = named_tuple_from_schema(row)

  print(nt)

  # provider = ExternalSchemaTransformProvider(
  #   BeamJarExpansionService(":sdks:java:io:expansion-service:shadowJar"))
  #
  # KafkaWriteTransform = provider.get("KafkaWrite")
  #
  # with beam.Pipeline() as p:
  #   p | beam.Create() | WriteToKafka()
  #
  # try:
  #   out = subprocess.run(['python', '-V'])
  #   out = subprocess.run([
  #     sys.executable,
  #     os.path.join('gen_xlang_wrappers.py'),
  #     '--cleanup',
  #     '--input-expansion-services', 'standard_expansion_services.yaml',
  #     '--output-transforms-config', 'standard_external_transforms.yaml'],
  #     capture_output=True, check=True)
  #   print(out.stdout)
  # except subprocess.CalledProcessError as err:
  #   raise RuntimeError('Could not generate external transform wrappers: %s', err.stderr)

  # from apache_beam.coders import RowCoder
  # row_coder = RowCoder.from_type_hint(RowTypeConstraint.from_fields([
  #   ('row', typing.Dict[str, int]),
  #   ('destination', str),
  #   ('schema', str)
  # ]), None)
  # content = {"row": {"what": 1}, "destination": "yerrr", "schema": "u got it"}
  # row = beam.Row(row={"what": 1}, destination="yerrr", schema="u got it")
  # row2 = beam.Row(**content)
  # bytesss = row_coder.encode(row)
  # bytesss2 = row_coder.encode(row2)
  # print(bytesss)
  # print(bytesss2)
  # print(bytesss == bytesss2)

  # with beam.Pipeline() as p:
  #   p | beam.Create([{"hi": 1}, {"hi": 2}]) | WriteToBigQuery(table=lambda x:"google.com:clouddfe:ahmedabualsaud_test.dynamic", schema="hi:INTEGER", method=WriteToBigQuery.Method.STORAGE_WRITE_API)

