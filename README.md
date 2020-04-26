# oetker-assessment
To test the producer and consumer, please download all the files including sample.json into one directory
Create the topic 'assessment' -  kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic assessment
or its equivalent for unix
Execute 'python consumer.py' followed by 'producer.py'
The code is tested with a sample of 10 records, out of which seven will be used by the consumer. Three records will be rejected due to incorrect format in IP address and email
Date format will always be - YYYY-MM-DD. Formats that are converted are - "%Y/%m/%d", "%d-%m-%Y", "%Y%m%d","%d/%m/%Y"
