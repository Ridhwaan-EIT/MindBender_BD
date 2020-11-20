from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

ACCESS_TOKEN= "1322200803453665281-hKQDXJVNbaZY4X1MrNN4LxuCgUnnwk"
TOKEN_SECRET= "Nlvth07MfqYYurULVsFpPVE5bhIl0CsdueJiV80AcoFjz"
CONSUMER_KEY= "1OxhdNfWyi1beCr8w2sgQz5uN"
CONSUMER_SECRET= "aOjkUURVVitj0KZjikrQFNK86RckGDhdrOvJ0dloyvojl4pr4D"
TOPIC = "Trump"

class StdOutListener(StreamListener):
   def on_data(self, data):
      producer.send_messages(TOPIC, data.encode('utf-8'))
      print (data, "Tweet sent" , sep='')
      return True
   def on_error(self, status):
      print (status)

kafka = KafkaClient("localhost:9099")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, TOKEN_SECRET)
stream = Stream(auth, l)
stream.filter(track=TOPIC)




