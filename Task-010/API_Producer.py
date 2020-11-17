from time import sleep
from json import dumps
import json
from kafka import KafkaProducer
import requests

producer = KafkaProducer(bootstrap_servers=['localhost:9094'])
body= {"token":"xoxb-846711423270-1513455157508-YWMoTGGIVstWq108VD1lrtwX","include_locale":"true","channel":"CQLGZMJKT"}
x= requests.get('https://slack.com/api/groups.info',params=body)
result=x.text
result_list= result.splitlines()


print(result)
for x in range(5):
	producer.send('kafka_spark', result.encode('utf-8'))
	print("SUCCESS")	




