from google.cloud import pubsub_v1

def publish(input, context):
    publisher = pubsub_v1.PublisherClient()
    topic = 'projects/splendid-sector-305218/topics/lab6'
    data = input['name'].encode("utf-8")
    publisher.publish(topic, data)