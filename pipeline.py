#importing needed modules
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging


def run(argv=None, save_main_session=True):  #pipeline method
    parser = argparse.ArgumentParser()
    parser.add_argument(#adding input and output arguments to command line arguments parser
        '--input',
        dest='input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)#parsing arguments

    def formatout(x):#method for formatting output
        return '{},{}'.format(x[0],x[2])


    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:#starting pipeline
        lines = (p | 'ReadData' >> beam.io.ReadFromAvro(known_args.input)#read data from avro
        | 'MapData' >> beam.Map(lambda record: (record['year'],(record['number'],record['name'])))#map data for next calculating of popular names 
        | 'FindingMax' >> beam.CombinePerKey(max)#calculating most popular names
        | 'MabData' >> beam.Map(lambda record: (record[0],record[1][0],record[1][1])))#preparing data
        output = lines | 'Format' >> beam.Map(formatout)#formatting output
        output | 'WriteToText' >> beam.io.WriteToText(known_args.output, file_name_suffix='.csv',num_shards=1,header='year,name')#writing output to .csv


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run() #running pipeline method