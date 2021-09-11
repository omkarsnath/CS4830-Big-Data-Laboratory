from google.cloud import pubsub_v1

subscriber = pubsub_v1.SubscriberClient()
subscription = 'projects/splendid-sector-305218/subscriptions/lab6-sub'

def count(input):
    print(input.data)
    print('Data Received Successfully')

    with open('sample1.txt', 'r') as file:
        lines = 0
        for line in file:
            lines = lines + 1

    print('Number of lines in file: ' + str(lines))
    input.ack()


runner = subscriber.subscribe(subscription, count)

try:
    runner.result()
except KeyboardInterrupt:
    runner.cancel()