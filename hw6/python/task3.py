import sys
import csv
import tweepy
import random

API_KEY = 'PGJY2i86PzLVDeDQDO8niSj4f'
API_SECRET_KEY = 'xI8EGPiFK3zlzH3TmsCGUIr7Y5XTSIfkukBhjUTVKR78cPYr9K'

ACCESS_TOKEN = '1210755856645492736-qJWIjBx8h43ykg507YzPzFOiSQVJBO'
ACCESS_TOKEN_SECRET = 'I0JHDy1xGukIlgx9C7n4B1biJ3CXSMPC9I8NFhbnOqyrq'

#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):

    def __init__(self, tweet_collection):
        tweepy.StreamListener.__init__(self)
        self.tweet_collection = tweet_collection

    def on_status(self, status):
        tweet_collection.handle_new_tweet(status)

class TweetCollection:
    def __init__(self, output_file_path, output_topN):
        self.tweet_dict = dict() # tweet_index : tweets_text
        self.tag_dict = dict() # tweet_index : tweet_tags
        self.MAX_NUM = 100
        self.output_file_path = output_file_path
        self.output_topN = output_topN

    def my_print(self):
        global tweets_index
        tag_num = dict()
        for tag_list in self.tag_dict.values():
            for tag in tag_list:
                tag_num[tag] = tag_num.setdefault(tag, 0) + 1
        tag_ordered = sorted(tag_num.items(), key=lambda x: x[0])
        tag_ordered = sorted(tag_ordered, key=lambda x: x[1], reverse=True)
        ordered_frequency = sorted(list(set(tag_num.values())), reverse=True)
        min_frequency = 0
        if len(ordered_frequency) > self.output_topN:
            min_frequency = ordered_frequency[self.output_topN-1]
        with open(self.output_file_path, 'a') as f:
            writer = csv.writer(f, delimiter=':')
            writer.writerow(["The number of tweets with tags from the beginning", tweets_index])
            for kv in tag_ordered:
                if kv[1] < min_frequency:
                    break
                writer.writerow(kv)
                # writer.writerow("{} : {}".format(kv[0], kv[1]))

            writer.writerow([])

    def add_tweet(self, tweet):
        global tweets_index
        tag_list = tweet.entities['hashtags']
        self.tweet_dict[tweets_index] = tweet.text
        tags = set()
        for tag_o in tag_list:
            tags.add(tag_o['text'])
        self.tag_dict[tweets_index] = tags
        print('n_tweets: {}, id: {}, tags: {}'.format(len(self.tweet_dict), tweets_index, ','.join(tags)))
        self.my_print()

    def handle_new_tweet(self, tweet):
        tag_list = tweet.entities['hashtags']
        if len(tag_list) != 0:
            # print(tag_list)
            global tweets_index
            tweets_index += 1
            if len(self.tweet_dict) < self.MAX_NUM:
                self.add_tweet(tweet)
            else:
                if random.random() <= self.MAX_NUM/tweets_index:
                    tweet_indices = list(self.tweet_dict.keys())
                    removed_key = random.choice(tweet_indices)
                    self.tweet_dict.pop(removed_key)
                    self.tag_dict.pop(removed_key)
                    self.add_tweet(tweet)



if __name__ == '__main__':
    output_file_path = "task3_ans.csv"
    # port, output_file_path = sys.argv[1:]
    output_topN = 3
    with open(output_file_path, 'w') as f:
        f.truncate()
    tweets_index = 0
    tweet_collection = TweetCollection(output_file_path, output_topN)
    myStreamListener = MyStreamListener(tweet_collection)
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    myStream = tweepy.Stream(auth=auth, listener=myStreamListener)
    myStream.filter(track=['python', 'C++', 'MachineLearning', 'AI'])

