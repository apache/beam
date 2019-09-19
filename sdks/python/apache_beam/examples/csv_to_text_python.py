# Import necessary modules
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# PipelineOptions declaration
options = PipelineOptions()
p = beam.Pipeline(options=options)

# PipelineOptions Arguments for Input and Output
class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input',
                            help='Input for the pipeline',
                            default='./data/')
        parser.add_argument('--output',
                            help='Output for the pipeline',
                            default='./output/')

# Procedure to split the data in the input csv
class Split(beam.DoFn):
    def process(self, element):
        TIMESTAMP,CUSTOMER,POINTNAME,VALUE = element.split(",")
        return [{
            'TIMESTAMP': datetime(TIMESTAMP),
            'CUSTOMER': string(CUSTOMER),
            'POINTNAME': string(POINTNAME),
            'VALUE': float(VALUE)
        }]

# Procedure to create tuples of the data from csv
class CollectOpen(beam.DoFn):
    def process(self, element):
        # Returns a list of tuples containing TIMESTAMP, CUSTOMER, POINTNAME, VALUE value from the .csv file
        result = [(1, element['TIMESTAMP'],
                   2, element['CUSTOMER'],
                   3, element['POINTNAME'],
                   4, element['VALUE'])]
        return result

# Input and Output filename declaration
input_filename = 'rh_201810.csv'
output_filename = 'result.txt'

# Using beam to read the text from input csv
csv_lines = (
    p | "inputn" >> beam.io.ReadFromText(input_filename) 
      | "splitn"    >> beam.ParDo(Split()) 
      | "collectn"  >> beam.ParDo(CollectOpen()
    )
)

# Write the output data to the text file
output = (
    p | "textoutput" >> beam.io.WriteToText(output_filename)
)
