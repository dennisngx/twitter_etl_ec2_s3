import pandas as pd
import json
from datetime import datetime
import tweepy
import s3fs

def twitter_etl():

    api_key = "*****************"
    api_secret = "*****************"
    access_key = "*****************"
    access_secret = "*****************"

    #Twitter Access
    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_key, access_secret)

    #Creating an API
    api = tweepy.API(auth)
    tweet_input = api.user_timeline(screen_name='@cz_binance', count=100, 
                                include_rts = True, tweet_mode = 'extended')

    tweet_list = []
    for tweet in tweet_input:
        text = tweet._json["full_text"]
        refined_tweet = {"user" : tweet.user.screen_name,
                        'text' : text,
                        'favorites' : tweet.favorite_count,
                        'retweets' : tweet.retweet_count,
                        'created_time' : tweet.created_at}
        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv("s3://twitter-etl-airflow-duong/CZ Twitter Data.csv")