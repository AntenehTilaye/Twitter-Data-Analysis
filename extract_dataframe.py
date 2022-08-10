import json
import pandas as pd
from textblob import TextBlob

from clean_tweets_dataframe import Clean_Tweets


def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))
    
    
    return len(tweets_data), tweets_data

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self)->list:
        try:
            statuses_count = [x["user"]['statuses_count'] for x in self.tweets_list]
        except KeyError:
            statuses_count = None

        return statuses_count
    
         
        
    def find_full_text(self)->list:
        try:
            text = [x['full_text'] for x in self.tweets_list]
        except KeyError:
            text = None

        return text
       
    
    def find_sentiments(self, text)->list:
        
        polarity = [TextBlob(x).polarity for x in text]
        
        subjectivity = [TextBlob(x).subjectivity for x in text]
        
        return polarity, subjectivity

    def find_created_time(self)->list:
        try:
            created_at = [x['created_at'] for x in self.tweets_list]
        except KeyError:
            created_at = None

        return created_at

    def find_source(self)->list:
        try:
            source = [x['source'] for x in self.tweets_list]
        except KeyError:
            source = None

        return source

    def find_screen_name(self)->list:
        try:
            screen_name = [x['user']['screen_name'] for x in self.tweets_list]
        except KeyError:
            screen_name = None

        return screen_name

    def find_followers_count(self)->list:
        try:
            followers_count = [x['user']['followers_count'] for x in self.tweets_list]
        except KeyError:
            followers_count = None

        return followers_count

    def find_friends_count(self)->list:
        try:
            friends_count = [x['user']['friends_count'] for x in self.tweets_list]
        except KeyError:
            friends_count = None

        return friends_count

    def is_sensitive(self)->list:
        try:
            is_sensitive = [x['possibly_sensitive'] if "possibly_sensitive" in x else None for x in self.tweets_list ]
        except KeyError:
            is_sensitive = None

        return is_sensitive

    def find_favourite_count(self)->list:
        try:
            favourites_count = [x['user']['favourites_count'] for x in self.tweets_list]
            # favourites_count = [x['favourites_count'] if "favourites_count" in x else None  for x in self.tweets_list ]
        except KeyError:
            favourites_count = None

        return favourites_count
        
    
    def find_retweet_count(self)->list:
        try:
            retweet_count = [x['retweet_count'] for x in self.tweets_list]
        except KeyError:
            retweet_count = None

        return retweet_count

    def find_hashtags(self)->list:
        try:
            hashtags = [x['entities']['hashtags'] for x in self.tweets_list]
        except KeyError:
            hashtags = None

        return hashtags

    def find_mentions(self)->list:
        try:
            mentions = [x['entities']['user_mentions'] for x in self.tweets_list]
        except KeyError:
            mentions = None

        return mentions


    def find_location(self)->list:
        try:
            location = [x['user']['location'] for x in self.tweets_list]
        except TypeError:
            location = None
        
        return location
    
    def find_lang(self)->list:
        try:
            langs = [x['lang'] for x in self.tweets_list]
            # langs = [TextBlob(x['full_text']).detect_language() for x in self.tweets_list]
        except TypeError:
            langs = None
        
        return langs
    
    def find_place_coord(self)->list:
        try:
            place_coord = [x["place"]['bounding_box']["coordinates"] if "bounding_box" in x else None for x in self.tweets_list ]
        except TypeError:
            place_coord = None
        
        return place_coord
    
    def find_listed_count(self)->list:
        try:
            listed_count = [x['user']['listed_count'] for x in self.tweets_list]
        except TypeError:
            listed_count = None
        
        return listed_count

    
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
            'original_author', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        
        
        data = zip(created_at, source, text, polarity, subjectivity, lang, fav_count, retweet_count, screen_name, follower_count, friends_count, sensitivity, hashtags, mentions, location)
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'original_text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    _, tweet_list = read_json("data/africa_twitter_data.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df(True) 

    # use all defined functions to generate a dataframe with the specified columns above
    
    sentiment = zip(tweet_df["polarity"], tweet_df["subjectivity"])
    place_cord = tweet.find_place_coord()
    screen_count = tweet.find_listed_count()
    clean_text = tweet.find_full_text()
    
    data = zip(tweet_df["created_at"], tweet_df["source"], tweet_df["original_text"], clean_text, sentiment, tweet_df["polarity"], 
               tweet_df["subjectivity"], tweet_df["lang"], tweet_df["favorite_count"], tweet_df["retweet_count"], 
               tweet_df["original_author"], screen_count, tweet_df["followers_count"], tweet_df["friends_count"], 
               tweet_df["possibly_sensitive"], tweet_df["hashtags"], tweet_df["user_mentions"], tweet_df["place"], place_cord)
    
    full_df = pd.DataFrame(data=data, columns=columns)
    
    cleaner = Clean_Tweets(tweet_df)
    clean_df = cleaner.get_clean_tweets()
    full_df.to_csv('cleaned_tweet_data.csv', index=False)
    