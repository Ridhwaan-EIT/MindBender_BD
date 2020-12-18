from kafka import SimpleProducer, KafkaClient
import requests

def makeQuery():
	with open("/home/fieldemployee/opt/key2.txt") as f:
		api_key = f.readline().rstrip()
	url = "https://aerodatabox.p.rapidapi.com/airports/icao/KDFW/stats/routes/daily"

	querystring = {"page":"1"}


	headers = {
            'x-rapidapi-key': api_key,
	    'x-rapidapi-host': "aerodatabox.p.rapidapi.com"
	    }

	response = requests.request("GET", url, headers=headers)

	print(response.text)
	return response.text


TOPIC = "American"
resp = makeQuery()

kafka = KafkaClient("localhost:9099")
producer = SimpleProducer(kafka)

producer.send_messages(TOPIC, resp.encode('utf-8'))
