# ME17B158 - CS4830 - Counting Clicks

import pyspark
import sys

if len(sys.argv) != 3:
  raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

inputUri=sys.argv[1]
outputUri=sys.argv[2]

sc = pyspark.SparkContext()
lines = sc.textFile(sys.argv[1])

def convert(datetime):
        time = datetime.split(" ")[1]
	hour = time.split(":")[0]
        
	if hour == 'Time':
                return 'Heading'  
        if int(hour) < 6:
                return '0-6'
        if int(hour) < 12:
                return '6-12'
        if int(hour) < 18:
                return '12-18'
        if int(hour) < 24:
                return '18-24'

times = lines.map(convert)
time_counts = times.map(lambda time: (time,1)).reduceByKey(lambda count1, count2: count1 + count2)
time_counts.coalesce(1).saveAsTextFile(sys.argv[2])