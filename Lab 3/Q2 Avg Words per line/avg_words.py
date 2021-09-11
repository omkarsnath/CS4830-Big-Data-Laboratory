import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
options = PipelineOptions()
google_cloud_options = options .view_as(GoogleCloudOptions)
google_cloud_options.project = 'splendid-sector-305218' 
google_cloud_options.region = "us-central1"
google_cloud_options.job_name = 'lab3q2'
google_cloud_options.temp_location = "gs://me17b158_cs4830/tmp"
options.view_as(StandardOptions).runner = "DataflowRunner"
with beam.Pipeline(options=options) as p:
        avgwords = p | 'Read' >> ReadFromText ('gs://iitmbd/out.txt') | 'Counting words per line' >> beam.Map (lambda x: len(re.split('[\s,;!]+',x))) | 'Taking mean' >> beam.CombineGlobally((beam.transforms.combiners.MeanCombineFn()) | 'Write to Text' >>  WriteToText('gs://me17b158_cs4830/lab3/')