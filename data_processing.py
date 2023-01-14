from pandas import DataFrame
import tweepy
from Twitter_API_Key import access_token,access_token_secret,consumer_key,consumer_secret

def run_elt():
    auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_token_secret)
    api = tweepy.API(auth)

    public_tweets = api.user_timeline(screen_name="@baovtuber", include_rts=True, count=1000, tweet_mode ="extend")

    tweets= []
    for tweet in public_tweets:
        refine_tweet = {
            "user_name": tweet.user.screen_name,
            "tweet": tweet.text,
            "favorite number": tweet.favorite_count,
            "retweeted number": tweet.retweet_count,
            "language": tweet.lang,
            "created_at": tweet.created_at
        }
        tweets.append(refine_tweet)

    df = DataFrame(tweets)
    df.to_csv("s3://my-test-airflow-bucket/twitter_data.csv",index=False)
run_elt()
