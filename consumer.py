from kafka import KafkaConsumer
import sys
import pandas as pd

bootstrap_servers = ['localhost:9092']
topicName = 'assessment'
topicCount='record_count'
consumer = KafkaConsumer (topicName, group_id = 'group1',bootstrap_servers = bootstrap_servers,auto_offset_reset = 'earliest')
consumer2 = KafkaConsumer (topicCount, group_id = 'group2',bootstrap_servers = bootstrap_servers,auto_offset_reset = 'earliest')
df = pd.DataFrame(columns=['id','first_name','last_name','email','gender','ip_address','date','country'])


try:
    for message in consumer:
        df_length = len(df)
        #df2 = pd.DataFrame(message.value.decode(),columns=['id','first_name','last_name','email','gender','ip_address','date','country'])
        #df.loc[df_length] = message.value.decode()
        distinct_country = df['country'].nunique()
        #print ("Distinct country count so far is" + str(distinct_country))
        incoming=message.value.decode()
        incoming=incoming[1:-1].replace('"','')
        
        mylist = incoming.split(",")
        df.loc[len(df),:] = mylist
        distinct_country = df['country'].nunique()
        print(df)
        print ("Distinct country count so far is " + str(distinct_country))
        
    for message in consumer2:
        count_sent = int(message.value.decode())
        if len(df) == count_sent:
            print("All records are received")
        else:
            print("Some records are lost")
    
except KeyboardInterrupt:
    sys.exit()