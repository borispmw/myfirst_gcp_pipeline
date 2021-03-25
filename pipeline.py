import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
#from apache_beam.io import ReadFromAvro
#from apache_beam.io import WriteToText


class MyOption(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input')
        parser.add_argument('--output')

with beam.Pipeline(options=PipelineOptions()) as p:
    lines = p | beam.io.ReadFromAvro('gs://temp_data_exam/usa*')
    lines | 'WriteToText' >> beam.io.WriteToText(output, file_name_suffix='.csv')
