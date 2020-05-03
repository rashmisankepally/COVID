"""This script uses the Twitter Streaming API, via the tweepy library,
to pull in tweets and publish them to a PubSub topic.
"""
#import datetime
from datetime import datetime, timezone
import pytz
import time
import json
import utils
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from google.cloud import pubsub_v1

import os
import sys

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

from countminsketch import CountMinSketch

# Configure the connection
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('bigdata-273220', 'got_COVID_tweets')
sketch = CountMinSketch(1000, 10)

# Function to write data to
def write_to_pubsub(data):
    try:
        if data["lang"] == "en":
            for hashtag in data["hashtags"]:
                print(hashtag)
                publisher.publish(topic_path, data=json.dumps({
                    "hashtag": hashtag,
                    #is it happening?
                    "frequency": sketch.estimate(hashtag),
                    "posted_at": datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=timezone.utc).astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')
                }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))
            """
            # publish to the topic, don't forget to encode everything at utf8!
            publisher.publish(topic_path, data=json.dumps({
                "text": data["text"],
                "user_id": data["user_id"],
                "id": data["id"],
                "hashtags": data["hashtags"]
                "posted_at": datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=timezone.utc).astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')
            }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))
            """
    except Exception as e:
        print(e)
        raise

# Method to format a tweet from tweepy
def reformat_tweet(tweet):
    x = tweet

    processed_doc = {
        "id": x["id"],
        "lang": x["lang"],
        "retweeted_id": x["retweeted_status"]["id"] if "retweeted_status" in x else None,
        "favorite_count": x["favorite_count"] if "favorite_count" in x else 0,
        "retweet_count": x["retweet_count"] if "retweet_count" in x else 0,
        "coordinates_latitude": x["coordinates"]["coordinates"][0] if x["coordinates"] else 0,
        "coordinates_longitude": x["coordinates"]["coordinates"][0] if x["coordinates"] else 0,
        "place": x["place"]["country_code"] if x["place"] else None,
        "user_id": x["user"]["id"],
        "created_at": x["created_at"]
        #"created_at": time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(x["created_at"], '%a %b %d %H:%M:%S +0000 %Y'))
        #"created_at": time.mktime(time.strptime(x["created_at"], "%a %b %d %H:%M:%S +0000 %Y"))
    }

    if x["entities"]["hashtags"]:
        processed_doc["hashtags"] = [y["text"] for y in x["entities"]["hashtags"]]
        for y in x["entities"]["hashtags"]:
            sketch.increment(y["text"])
    else:
        processed_doc["hashtags"] = []

    if x["entities"]["user_mentions"]:
        processed_doc["usermentions"] = [{"screen_name": y["screen_name"], "startindex": y["indices"][0]} for y in
                                         x["entities"]["user_mentions"]]
    else:
        processed_doc["usermentions"] = []

    if "extended_tweet" in x:
        processed_doc["text"] = x["extended_tweet"]["full_text"]
    elif "full_text" in x:
        processed_doc["text"] = x["full_text"]
    else:
        processed_doc["text"] = x["text"]

    return processed_doc


class StdOutListener(StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a PubSub topic
    """
    count = 0
    twstring = ''
    tweets = []
    batch_size = 50
    total_tweets = 100000
    client = utils.create_pubsub_client(utils.get_credentials())

    def on_status(self, data):
        write_to_pubsub(reformat_tweet(data._json))
        self.count += 1
        # if we've grabbed more than total_tweets tweets, exit the script.
        if self.count > self.total_tweets:
            return False
        return True
    '''
    def on_data(self, data):
        """What to do when tweet data is received."""
        self.tweets.append(data)
        if len(self.tweets) >= self.batch_size:
            write_to_pubsub(reformat_tweet(data._json))
            self.tweets = []
        if self.count > self.total_tweets:
            return False
        if (self.count % 1000) == 0:
            print('count is: %s at %s' % (self.count, datetime.now()))
        return True
    '''

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    listener = StdOutListener()
    auth = OAuthHandler('MuDx4Cuoq2W5pr463fi1vivf1', 'mU5Vlmpu63vNPHYfm5AZOe7FlGB8RHEuwsxHUmtOb2I71h1O8J')
    auth.set_access_token('165781656-CRCOOZ6UWZD0rYNNwRKcTZKo5ztTd0a7MEeReGzg', 'aklRMf4da3A0OIYZjpPuwUBZzzQewY3yp5GKylx3mMarD')
    #api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=False)
    stream = Stream(auth, listener)
    stream.filter(track=['coronavirus', 'COVID', 'COVID19', 'COVID-19', 'COVID_19'], is_async=True)

