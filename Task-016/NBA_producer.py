from kafka import SimpleProducer, KafkaClient
import requests

def makeQuery():
	with open("/home/fieldemployee/opt/key.txt") as f:
		api_key = f.read()
	url = "https://free-nba.p.rapidapi.com/teams"

	querystring = {"page":"1"}


	headers = {
	    'x-rapidapi-key': "3e2a3c832dmsh1cf38aaf2544186p1d3d68jsn337e112c9dfd",
	    #'x-rapidapi-key': api_key,
	    'x-rapidapi-host': "free-nba.p.rapidapi.com"
	    }

	response = requests.request("GET", url, headers=headers, params=querystring)

	print(response.text)
	return response.text


TOPIC = "West"
resp = makeQuery()

kafka = KafkaClient("localhost:9099")
producer = SimpleProducer(kafka)

producer.send_messages(TOPIC, resp.encode('utf-8'))
