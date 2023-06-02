import apache_beam as beam
from apache_beam.transforms.external import SchemaAwareExternalTransform


with beam.Pipeline() as p:
  _ = (
      p
      | SchemaAwareExternalTransform(
          identifier="beam:schematransform:org.apache.beam:other_wordcount:v1",
          expansion_service="localhost:8099",
          inputFile="gs://apache-beam-samples/shakespeare/kinglear.txt")
      | beam.Map(lambda row: print(row.wordCount)))
