
import pandas as pd
from textblob import TextBlob

class Clean_Tweets:
    """
    The PEP8 Standard AMAZING!!!
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')
        
    def drop_unwanted_column(self)->pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = self.df[self.df['retweet_count'] == 'retweet_count' ].index
        self.df.drop(unwanted_rows , inplace=True)
        
        return self.df
        
        
    def drop_duplicate(self)->pd.DataFrame:
        """
        drop duplicate rows
        """
        
        self.df.drop_duplicates(subset=["original_text"], keep="first")
        
        return self.df
    
    def convert_to_datetime(self)->pd.DataFrame:
        """
        convert column to datetime
        """
        
        self.df['created_at'] = pd.to_datetime(self.df['created_at'], format='%a %b %d %H:%M:%S %z %Y')

        
        self.df = self.df[self.df['created_at'] >= '2020-12-31' ]
        
        return self.df
    
    def convert_to_numbers(self)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        
        for name in list(self.df.select_dtypes(include=['int16', 'int32', 'int64', 'float16', 'float32', 'float64']).columns.values):
            self.df[name] = pd.to_numeric(self.df[name], errors="coerce")
            
        # self.df['subjectivity'] = pd.to_numeric(self.df['subjectivity'], errors="coerce")
        # self.df['retweet_count'] = pd.to_numeric(self.df['retweet_count'], errors="coerce")
        # self.df['favorite_count'] = pd.to_numeric(self.df['favorite_count'], errors="coerce")
                
        return self.df
        
        

    def remove_non_english_tweets(self)->pd.DataFrame:
        """
        remove non english tweets from lang
        """
        
        self.df = self.df[self.df['original_text'].map(lambda x: x.isascii())]
        
        
        return self.df
        
        
    
    def clean_tweets(self)->pd.DataFrame:
        self.drop_unwanted_column()
        self.drop_duplicate()
        self.convert_to_datetime()
        self.convert_to_numbers()
        self.remove_non_english_tweets()
            
        return self.df