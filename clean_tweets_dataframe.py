
import pandas as pd
from textblob import TextBlob

class Clean_Tweets:
    """
    The PEP8 Standard AMAZING!!!
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')
        
    def drop_unwanted_column(self):
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = self.df[self.df['retweet_count'] == 'retweet_count' ].index
        self.df.drop(unwanted_rows , inplace=True)
        # self.df = self.df[self.df['polarity'] != 'polarity']
        
    def drop_duplicate(self):
        """
        drop duplicate rows
        """
        
        self.df.drop_duplicates(subset=["id"])
        
        return self.df
    
    def convert_to_datetime(self):
        """
        convert column to datetime
        """
        
        self.df['created_at'] = pd.to_datetime(self.df['created_at'], format='%Y%m%d')

        
        self.df = self.df[self.df['created_at'] >= '2020-12-31' ]
        
        return self.df
    
    def convert_to_numbers(self):
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        for name in list(self.df.select_dtypes(include=['int16', 'int32', 'int64', 'float16', 'float32', 'float64']).columns.values):
            self.df[name] = pd.to_numeric(self.df[name], errors="coerce")
            
        # self.df['subjectivity'] = pd.to_numeric(self.df['subjectivity'], errors="coerce")
        # self.df['retweet_count'] = pd.to_numeric(self.df['retweet_count'], errors="coerce")
        # self.df['favorite_count'] = pd.to_numeric(self.df['favorite_count'], errors="coerce")
        
        
        
    
    def remove_non_english_tweets(self):
        """
        remove non english tweets from lang
        """
        
        self.df = self.df[self.df["original_text"].isascii()]
        
        
    
    def get_clean_tweets(self):
            self.drop_unwanted_column()
            self.drop_duplicate()
            self.convert_to_datetime()
            self.convert_to_numbers()
            self.remove_non_english_tweets()