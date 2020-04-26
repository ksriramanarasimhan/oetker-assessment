from time import sleep
from json import dumps
from kafka import KafkaProducer
import pandas as pd
import json
import datetime
#from pandas.io.json import json_normalize
#from pyspark.sql import SparkSession

def guess_date(string):
    for fmt in ["%Y/%m/%d", "%d-%m-%Y", "%Y%m%d","%d/%m/%Y"]:
        try:
            return datetime.datetime.strptime(string, fmt).date()
        except ValueError:
            continue
    raise ValueError(string)

def validate_ip(s):
    a = s.split('.')
    if len(a) != 4:
        return False
    for x in a:
        if not x.isdigit():
            return False
        i = int(x)
        if i < 0 or i > 255:
            return False
    return True


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
df = pd.DataFrame(columns=['id','first_name','last_name','email','gender','ip_address','date','country'])

with open("sample.json") as file:
    data = json.load(file)
    
df = (pd.json_normalize(data))

list_to_send = []
count_of_records = 0

for i in range(len(df)) :
    
    list_to_send.append(str(df.loc[i,"id"]))
    list_to_send.append((df.loc[i,"first_name"]).upper())
    list_to_send.append((df.loc[i,"last_name"]).upper())
    list_to_send.append(df.loc[i,"email"])
    list_to_send.append(df.loc[i,"gender"])
    list_to_send.append(df.loc[i,"ip_address"])
    list_to_send.append(str(guess_date(df.loc[i,"date"])))
    list_to_send.append(df.loc[i,"country"])

    if validate_ip(df.loc[i,"ip_address"]) and '@' in df.loc[i,"email"] and '.' in df.loc[i,"email"]:
        producer.send('assessment', list_to_send)
        list_to_send = []
        count_of_records+=1
    
    else:
        print(list_to_send)
        list_to_send = []

producer.send('record_count',count_of_records)
