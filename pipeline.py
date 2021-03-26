import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
#from apache_beam.io import ReadFromAvro
#from apache_beam.io import WriteToText


def formatout(year, num, name):
    return '{},{},{}'.format(year,num,name)


with beam.Pipeline(options=PipelineOptions()) as p:
    lines = (p | 'ReadData' >> beam.io.ReadFromAvro('gs://temp_data_exam/usa.avro')
    | 'MapData' >> beam.Map(lambda record: (record['year'],(record['number'],record['name'])))
    | 'FindingMax' >> beam.CombinePerKey(max)
    | 'MabData' >> beam.Map(lambda record: (record[0],record[1][0],record[1][1])))
    output = lines | 'Format' >> beam.Map(formatout)
    output | 'WriteToText' >> beam.io.WriteToText('gs://temp_data_exam/output_folder/output', file_name_suffix='.csv',mum_shards=1,header='year,number,name')
