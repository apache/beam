import apache_beam as beam
from apache_beam.transforms.external_transform_provider import ExternalTransformProvider
from apache_beam.transforms.external import BeamJarExpansionService

provider = ExternalTransformProvider(BeamJarExpansionService("sdks:java:io:expansion-service:shadowJar"))
MqttRead = provider.get_urn("beam:schematransform:org.apache.beam:mqtt_read:v1")
MqttWrite = provider.get_urn("beam:schematransform:org.apache.beam:mqtt_write:v1")

with beam.Pipeline() as p:
  connection_configuration = {
    "server_uri": "tcp://localhost:58494",
    "topic": "WRITE_TOPIC",
    "client_id": "READ_PIPELINE"
  }

  # read
  p | MqttRead(connection_configuration, max_read_time_seconds=10) | beam.Map(print)

  # write
  # p | beam.Create([beam.Row(bytes=bytes([1, 2, 3, 4, 5]))]) | MqttWrite(
  #       connection_configuration=connection_configuration)