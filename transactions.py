import argparse
import datetime
import logging
import apache_beam as beam

from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

def parse_csv(elements):
    return elements.split(",")

def get_greater_than_20(parsedline):
    if(float(parsedline[3]) > 20):
       return parsedline

def get_transactions_after_2009(greater_than_20):
    date = datetime.datetime.strptime(greater_than_20[0], '%Y-%m-%d %H:%M:%S UTC')
    if(date.year >= 2010):
        return [date.date().isoformat(), float(greater_than_20[3])]

def sum_total(all_filtered):
    added_values = sum(v for name, v in all_filtered)
    result_combined = [all_filtered[0][0], added_values]
    return result_combined

def format_json_lines(final_data):
    each_row = {"date": str(final_data[0]), "total_amount": str(final_data[1])}
    return each_row

def filter_out_nones(row):
  if row is not None:
    yield row
'''Groups all transformations into one composite transformation'''
class MyCompositeTransform(beam.PTransform):

    def expand (self, input):

        transformed = (
            input
                | 'ParseCSV' >> beam.Map(parse_csv)
                | 'GreaterThan20' >> beam.Filter(get_greater_than_20)
                | beam.ParDo(filter_out_nones)
                | 'After2009' >> beam.Map(get_transactions_after_2009)
                | 'FilterOutSecondTime' >> beam.ParDo(filter_out_nones)
                | 'GroupByDate' >> beam.GroupBy(lambda d: d[0])
                | 'GetAllGrouped' >> beam.Map(lambda s: s[1])
                | 'Sum' >> beam.Map(sum_total)
                | beam.Map(format_json_lines)
               )
        return transformed

def run(argv=None):
  """Main pipeline with default pointing to sample location"""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv',
      help='Input file to process.e.g. 1: `python transactions.py` or 2: `python transactions.py --input transactions.csv --output results-sum-from-local`')
  parser.add_argument(
      '--output',
      dest='output',
      required=False,
      default='results',
      help='Output file to write results to output folder')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=pipeline_options) as p: (
            p
            | 'Read' >> ReadFromText(known_args.input, skip_header_lines=1)
            | "CallMyCompositeTransformation" >> MyCompositeTransform()
            | "WriteJsonl" >> beam.io.WriteToText("output/" + known_args.output,file_name_suffix=".jsonl.gz", shard_name_template='')
        )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
