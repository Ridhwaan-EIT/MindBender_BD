import requests
import json
from datetime import datetime as dt
from kafka import SimpleProducer
from kafka import KafkaClient

start = dt.now()
start_stamp = dt.timestamp(start)

#set client id and secret, and authenitcate to obtain access token

client_id = "8pkpdjorj5tdm2wmf05onm3v1uhctr"
client_secret = "318fzytwatmob4iinlor7sladqnsdz"
POST_URL = "https://id.twitch.tv/oauth2/token?client_id=" + client_id + "&client_secret=" + client_secret + "&grant_type=client_credentials"

#get url id map from localfile
##
urlkey = {}
url_count = 0
with open("urlkey.txt", "r") as thisfile:
    for line in thisfile:
        url_count += 1
        split = line.split(',')
        urlkey[int(split[0])] = str(split[1].strip("\n"))
##
#POST request - obtain access token using client id and secret

message = []
data = {}
print("Obtaining access token")
auth_response = requests.post(POST_URL)
auth_response_json = auth_response.json()
access_token = auth_response_json['access_token']
print("Access token acquired, requesting data. This will take some time...")

#change these variable values to alter search parameters - upper bound for limit is 500

endpoint_URL = "https://api.igdb.com/v4/games"
limit = 500

#Generate and sumbit request for data

headers = {"Authorization": "Bearer {token}".format(token=access_token), "Client-ID": client_id}
parameters = "fields name,first_release_date,genres,rating,hypes,follows; where rating > 1 & first_release_date!= null & first_release_date < " + str(start_stamp) + " & genres != null; limit " + str(limit) + "; sort first_release_date desc;"#changed 500 to variable to ref later, genres.name
games = requests.post(endpoint_URL, data=parameters, headers=headers)
games = games.json()

            #if key in game['genres.id']:
                #rating_sum += game['rating']
                #try:
                    #hype_sum += game['hypes']
                #except:
                    #pass
        
#print message to check format, and store data at broker (port 9097)

kafka = KafkaClient("localhost:9099")
producer = SimpleProducer(kafka)
#for game in games:
    #message = json.dumps(game)#whattosend
    #producer.send_messages("igdbdata", message.encode("utf-8"))
    #print("Data sent successfully")
message = json.dumps(games)#whattosend
producer.send_messages("igdbdata", message.encode("utf-8"))
print("Data sent successfully")
end = dt.now()
end_stamp = dt.timestamp(end)
difference = end_stamp - start_stamp
log = "FIRST RUN - " + str(start_stamp) + ", END - " + str(end_stamp) + ", duration " + str(difference) + "ms\n"
with open("igdbAPIlogs.txt", "a+") as thisfile:
    thisfile.write(log)
    thisfile.write(message)
    thisfile.write("\n")
print("Logs saved")
print(games)
