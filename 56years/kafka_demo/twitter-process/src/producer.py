from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import io
import avro.datafile
import avro.io
import avro.ipc
import avro.schema

access_token = "936766436772663297-lDZ1AyP3z6NiZ1L0qQLQdo5PQXW6VZR"
access_token_secret = "NY1x4ZIdTjBDgfMAIgknz1urSPE3AZK2tDPEwyXIl3ovS"
consumer_key = "7Dzvyp5IKeB0dgj9wdcBEP2Fi"
consumer_secret = "eTxZwEydglZeQIZguMiYcaQokRzNA04MG1mOY2uwEwMEZhxNd9"
topic = "data_raw"


SCHEMA = avro.schema.parse(
    json.dumps(
        {"namespace": "avro",
        "type": "record",
        "name":"Twit",
        "fields": [
            {"name": "created_at", "type": "string"},
            {"name": "id", "type": "int"},
            {"name": "text", "type": "string"},
            {"name": "location", "type": "string"},
            {"name": "name", "type": "string"}
        ]
        }
    )
)


class StdOutListener(StreamListener):

    def format_data(self, created_at, id, text, location, name):
        print"Created at: " + created_at
        print"id: " + id
        print"text: " + text
        print"location: " + location
        print"name: " + name
        return "Fin"

    def on_data(self, data):
        parsed_data = json.loads(data)
        formated_data = format_data(
            parsedData["created_at"], parsedData["id"], 
            parsedData["text"], parsedData["user"]["location"],
            parsedData["user"]["screen_name"]
            )
        print(formatedData)
        #producer.send_messages(topic, formatedData.encode('utf-8'))
        return True

    def onError(self, status):
        print status


if __name__ == '__main__':
    #kafka = KafkaClient("localhost:9092")
    #producer = SimpleProducer(kafka)
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=['pokemon', 'kafka', 'zelda', 'christmas', 'politics', 
    'memes', 'mathematics', 'elon musk'])

