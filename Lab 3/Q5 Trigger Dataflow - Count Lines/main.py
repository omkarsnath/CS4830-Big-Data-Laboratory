def dataflow_count_lines(data, context):
    from uuid import uuid4
    import apache_beam as beam
    from apache_beam.io import ReadFromText
    from apache_beam.io import WriteToText
    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.options.pipeline_options import GoogleCloudOptions
    from apache_beam.options.pipeline_options import StandardOptions
    file_path = f"gs://{data['bucket']}/{data['name']}"
    unique_id = f"{data['name'].split('.')[0]}-{uuid4()}"
    output_path = f"gs://me17b158_cs4830/lab3/"
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "splendid-sector-305218"
    google_cloud_options.job_name = f"{unique_id}"
    google_cloud_options.temp_location="gs://me17b158_cs4830/tmp/"
    options.view_as(StandardOptions).runner = "DataflowRunner"
    google_cloud_options.region="us-central1"
    with beam.Pipeline(options=google_cloud_options) as p:
        lines = p | 'Read' >> ReadFromText(file_path)
        counts = lines | 'Count elements' >> beam.combiners.Count.Globally()
        output = counts
        output | 'Write' >> WriteToText(output_path)