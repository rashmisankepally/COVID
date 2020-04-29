"""This script uses the Twitter Streaming API, via the tweepy library,
to pull in tweets and publish them to a PubSub topic.
"""
import datetime
import utils
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from google.cloud import pubsub_v1

# Configure the connection
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('bigdata-273220', 'got_COVID_tweets')

# Function to write data to
def write_to_pubsub(data):
    try:
        if data["lang"] == "en":

            # publish to the topic, don't forget to encode everything at utf8!
            publisher.publish(topic_path, data=json.dumps({
                "text": data["text"],
                "user_id": data["user_id"],
                "id": data["id"],
                "posted_at": datetime.datetime.fromtimestamp(data["created_at"]).strftime('%Y-%m-%d %H:%M:%S')
            }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))

    except Exception as e:
        print(e)
        raise


class StdOutListener(StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a PubSub topic
    """
    count = 0
    twstring = ''
    tweets = []
    batch_size = 50
    total_tweets = 1000000
    client = utils.create_pubsub_client(utils.get_credentials())

    def on_status(self, data):
        write_to_pubsub(reformat_tweet(data._json))
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
            print('count is: %s at %s' % (self.count, datetime.datetime.now()))
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

