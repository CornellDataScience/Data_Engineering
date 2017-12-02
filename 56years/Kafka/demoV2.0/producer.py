#kafka-python
#python-twitter
#tweepy

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

topic = ""
search = ""
access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""

class out(StreamListener):
    def onVerify(self, data):
        producer.send_messages(topic, data.encode('utf-8'))
        print(data)
        return True
    def error(self, status):
        print(status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
listen = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, listen)
stream.filter(track=search)