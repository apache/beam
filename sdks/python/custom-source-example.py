import logging
import sys
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.lyft.kafka import FlinkKafkaInput
from apache_beam.io.lyft.kinesis import FlinkKinesisInput
from apache_beam.options.pipeline_options import PipelineOptions

class LogFn(beam.DoFn):
  def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
    import time
    logging.warn(('RESULT: {} ({}-{})\t{}\n\n'.format(timestamp, window.start, window.end, str(element))))
    yield element

if __name__ == "__main__":
  options_string = sys.argv.extend([
      "--runner=PortableRunner",
      "--job_endpoint=localhost:8099",
      "--parallelism=1",
      "--streaming"
  ])
  pipeline_options = PipelineOptions(options_string)

  # To run with local Kinesalite:
  # docker run -d --name mykinesis -p 4567:4567 instructure/kinesalite
  # AWS_ACCESS_KEY_ID=x; AWS_SECRET_ACCESS_KEY=x
  # aws kinesis create-stream --endpoint-url http://localhost:4567/ --stream-name=beam-example --shard-count=1
  # aws kinesis put-record --endpoint-url http://localhost:4567/ --stream-name beam-example --partition-key 123 --data 'count the words'
  # export AWS_CBOR_DISABLE=1

  with beam.Pipeline(options=pipeline_options) as p:
    (p
        # | 'Create' >> beam.Create(['hello', 'world', 'world'])
        # | 'Read' >> ReadFromText("gs://dataflow-samples/shakespeare/kinglear.txt")
        | 'Kafka' >> FlinkKafkaInput().with_topic('beam-example').with_bootstrap_servers('localhost:9092').with_group_id('beam-example-group').with_max_out_of_orderness_millis(10000)
        # | 'Kinesis' >> FlinkKinesisInput().with_stream('beam-example').with_endpoint('http://localhost:4567', 'fakekey', 'fakesecret')
        | 'reshuffle' >> beam.Reshuffle()
        #| 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        #                .with_output_types(unicode))
        #| 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        #| 'GroupAndSum' >> beam.CombinePerKey(sum)
        #| beam.Map(lambda x: logging.info("Got record: %s", x) or (x, 1))
        | beam.ParDo(LogFn())
     )
