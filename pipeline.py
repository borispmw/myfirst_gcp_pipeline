import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging

def run(argv=None, save_main_session=True):  
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    def formatout(x):
        return '{},{}'.format(x[0],x[2])


    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        lines = (p | 'ReadData' >> beam.io.ReadFromAvro(known_args.input)#'gs://temp_data_exam/usa.avro')
        | 'MapData' >> beam.Map(lambda record: (record['year'],(record['number'],record['name'])))
        | 'FindingMax' >> beam.CombinePerKey(max)
        | 'MabData' >> beam.Map(lambda record: (record[0],record[1][0],record[1][1])))
        output = lines | 'Format' >> beam.Map(formatout)
        output | 'WriteToText' >> beam.io.WriteToText(known_args.output, file_name_suffix='.csv',num_shards=1,header='year,name')#'gs://temp_data_exam/output_folder/output', file_name_suffix='.csv',num_shards=1,header='year,number,name')

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()